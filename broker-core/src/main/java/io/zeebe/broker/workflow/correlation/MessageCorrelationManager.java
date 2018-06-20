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

import static io.zeebe.util.buffer.BufferUtil.wrapString;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.topology.*;
import io.zeebe.broker.message.record.MessageSubscriptionRecord;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.clientapi.*;
import io.zeebe.protocol.intent.MessageSubscriptionIntent;
import io.zeebe.transport.*;
import io.zeebe.util.buffer.BufferWriter;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;
import org.slf4j.Logger;

public class MessageCorrelationManager implements TopologyPartitionListener {

    private static final Logger LOG = Loggers.STREAM_PROCESSING;

    private static final Duration FETCH_TOPICS_TIMEOUT = Duration.ofSeconds(15);

  private final Map<String, IntArrayList> topicPartitions = new HashMap<>();

  private final FetchCreatedTopicsRequest fetchCreatedTopicsRequest = new FetchCreatedTopicsRequest();
  private final FetchCreatedTopicsResponse fetchCreatedTopicsResponse = new FetchCreatedTopicsResponse();

  private MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private ExecuteCommandRequestEncoder encoder = new ExecuteCommandRequestEncoder();

  private final ClientTransport managementClient;
  private final ClientTransport clientApiClient;
  private final TopologyManager topologyManager;

  private final ActorControl actor;

  private volatile RemoteAddress systemTopicLeaderAddress;

  private final Int2ObjectHashMap<RemoteAddress> partitionLeaders = new Int2ObjectHashMap<>();

  private final MessageDigest md;

  private final int currentPartitionId;

  public MessageCorrelationManager(
      ClientTransport managementClient,
      ClientTransport clientApiClient,
      TopologyManager topologyManager,
      ActorControl actor,
      int partitionId) {
    this.managementClient = managementClient;
    this.clientApiClient = clientApiClient;
    this.topologyManager = topologyManager;
    this.actor = actor;
    this.currentPartitionId = partitionId;

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

    return managementClient
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

  public ActorFuture<ClientResponse> openSubscription(int partitionId, String eventKey, String messageName, long workflowInstanceKey, long activityInstanceKey)
  {

      final MessageSubscriptionRecord sub = new MessageSubscriptionRecord();
      sub.setMessageName(wrapString(messageName))
          .setMessageKey(wrapString(eventKey))
          .setWorkflowInstanceKey(workflowInstanceKey)
          .setActivityInstaneId(activityInstanceKey)
          .setPartitionId(currentPartitionId);

      LOG.info("open message subscription on partition '{}': {}", partitionId, sub);

      final MessageSubscriptionRequest request = new MessageSubscriptionRequest(sub, partitionId);

      if (!partitionLeaders.containsKey(partitionId))
      {
          return CompletableActorFuture.completedExceptionally(new RuntimeException("no leader found for partition with id: " + partitionId));
      }
      else
      {
          final RemoteAddress remoteAddress = partitionLeaders.get(partitionId);
          return clientApiClient.getOutput().sendRequest(remoteAddress, request, Duration.ofSeconds(5));
      }
  }

  class MessageSubscriptionRequest implements BufferWriter
  {
    private final MessageSubscriptionRecord record;
    private final int partitionId;

    MessageSubscriptionRequest(MessageSubscriptionRecord record, int partitionId)
    {
        this.record = record;
        this.partitionId = partitionId;
    }

      @Override
    public void write(MutableDirectBuffer buffer, int offset)
    {
          headerEncoder
              .wrap(buffer, offset)
              .blockLength(encoder.sbeBlockLength())
              .schemaId(encoder.sbeSchemaId())
              .templateId(encoder.sbeTemplateId())
              .version(encoder.sbeSchemaVersion());

          offset += headerEncoder.encodedLength();

          encoder.wrap(buffer, offset);

          encoder
              .partitionId(partitionId)
              .sourceRecordPosition(ExecuteCommandRequestEncoder.sourceRecordPositionNullValue())
              .position(ExecuteCommandRequestEncoder.positionNullValue())
              .key(ExecuteCommandRequestEncoder.keyNullValue());

          encoder.valueType(ValueType.MESSAGE_SUBSCRIPTION);
          encoder.intent(MessageSubscriptionIntent.SUBSCRIBE.getIntent());

          offset = encoder.limit();

          buffer.putShort(offset, (short) record.getLength(), ByteOrder.LITTLE_ENDIAN);

          offset += ExecuteCommandRequestEncoder.valueHeaderLength();

          record.write(encoder.buffer(), offset);
    }

      @Override
    public int getLength()
    {
        return MessageHeaderEncoder.ENCODED_LENGTH
                + ExecuteCommandRequestEncoder.intentEncodingLength()
                + ExecuteCommandRequestEncoder.keyEncodingLength()
                + ExecuteCommandRequestEncoder.partitionIdEncodingLength()
                + ExecuteCommandRequestEncoder.positionEncodingLength()
                + ExecuteCommandRequestEncoder.sourceRecordPositionEncodingLength()
                + ExecuteCommandRequestEncoder.valueTypeEncodingLength()
                + ExecuteCommandRequestEncoder.valueHeaderLength()
                + record.getLength();

    }
  }

    @Override
    public void onPartitionUpdated(PartitionInfo partitionInfo, NodeInfo member)
    {
        final RemoteAddress currentLeader = systemTopicLeaderAddress;

        if (member.getLeaders().contains(partitionInfo))
        {
            if (partitionInfo.getPartitionId() == Protocol.SYSTEM_PARTITION)
            {
                final SocketAddress managementApiAddress = member.getManagementApiAddress();
                if (currentLeader == null || currentLeader.getAddress().equals(managementApiAddress))
                {
                    systemTopicLeaderAddress = managementClient.registerRemoteAddress(managementApiAddress);
                }
            }
            else
            {
                actor.submit(() ->
                {
                    final SocketAddress clientApiAddress = member.getClientApiAddress();
                    final RemoteAddress remoteAddress = clientApiClient.registerRemoteAddress(clientApiAddress);

                    partitionLeaders.put(partitionInfo.getPartitionId(), remoteAddress);
                });
            }
        }
    }

}
