/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.workflow.correlation;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.topology.*;
import io.zeebe.protocol.Protocol;
import io.zeebe.transport.*;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import org.agrona.DirectBuffer;
import org.agrona.collections.IntArrayList;
import org.slf4j.Logger;

public class MessageCorrelationManager implements TopologyPartitionListener {

    private static final Logger LOG = Loggers.STREAM_PROCESSING;

    private static final Duration FETCH_TOPICS_TIMEOUT = Duration.ofSeconds(15);

  private final Map<String, IntArrayList> topicPartitions = new HashMap<>();

  private final FetchCreatedTopicsRequest fetchCreatedTopicsRequest = new FetchCreatedTopicsRequest();
  private final FetchCreatedTopicsResponse fetchCreatedTopicsResponse = new FetchCreatedTopicsResponse();

  private final ClientTransport clientTransport;
  private final TopologyManager topologyManager;

  private final ActorControl actor;

  private volatile RemoteAddress systemTopicLeaderAddress;

  private final MessageDigest md;

  public MessageCorrelationManager(
      ClientTransport clientTransport,
      TopologyManager topologyManager,
      ActorControl actor) {
    this.clientTransport = clientTransport;
    this.topologyManager = topologyManager;
    this.actor = actor;

    topologyManager.addTopologyPartitionListener(this);

    try {
        md = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException("Failed to initialize hash function", e);
      }
  }

  public void close() {
    topologyManager.removeTopologyPartitionListener(this);
  }

  private ActorFuture<ClientResponse> sendFetchTopicsRequest() {

    return clientTransport
        .getOutput()
        .sendRequestWithRetry(
            this::systemTopicLeader, this::checkResponse, fetchCreatedTopicsRequest, FETCH_TOPICS_TIMEOUT);
  }

  private boolean checkResponse(DirectBuffer response)
  {
      return !fetchCreatedTopicsResponse.tryWrap(response);
  }

  private RemoteAddress systemTopicLeader() {
    return systemTopicLeaderAddress;
  }

  public ActorFuture<Void> fetchTopics()
  {
      final CompletableActorFuture<Void> future = new CompletableActorFuture<>();

       LOG.info("Fetch topics");

      actor.runOnCompletion(sendFetchTopicsRequest(), (response, failure) ->
      {
          // TODO handle timeout and what happen if the partition doesn't exist
          if (failure == null)
          {
              fetchCreatedTopicsResponse.wrap(response.getResponseBuffer());

              fetchCreatedTopicsResponse.getTopics().forEach(topic ->
              {
                  topicPartitions.put(topic.getTopicName(), topic.getPartitionIds());
              });

              LOG.info("topics: {}", topicPartitions);

              future.complete(null);
          }
          else
          {
              LOG.error("Failed to fetch topics", failure);

              future.completeExceptionally(failure);
          }
      });

      return future;
  }

  public int getPartitionForTopic(String eventTopic, String eventKey)
  {
      if (topicPartitions.containsKey(eventTopic))
      {
          final IntArrayList partitionIds = topicPartitions.get(eventTopic);

          md.reset();
          md.update(eventKey.getBytes(StandardCharsets.UTF_8));
          final byte[] digest = md.digest();

          final int hashCode = Math.abs(Arrays.hashCode(digest));

          final int eventPartition = hashCode % partitionIds.size();

          return partitionIds.getInt(eventPartition);
      }
      else
      {
          return -1;
      }
  }

  public void openSubscription(int partitionId, String eventKey)
  {
      LOG.info("open message subscription on partition '{}'", partitionId);

      // TODO open message subscription
  }

  @Override
  public void onPartitionUpdated(PartitionInfo partitionInfo, NodeInfo member) {
    final RemoteAddress currentLeader = systemTopicLeaderAddress;

    if (partitionInfo.getPartitionId() == Protocol.SYSTEM_PARTITION) {
      if (member.getLeaders().contains(partitionInfo)) {
        final SocketAddress managementApiAddress = member.getManagementApiAddress();
        if (currentLeader == null || currentLeader.getAddress().equals(managementApiAddress)) {
          systemTopicLeaderAddress = clientTransport.registerRemoteAddress(managementApiAddress);
        }
      }
    }
  }
}
