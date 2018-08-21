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
package io.zeebe.broker.system.workflow.repository.processor;

import static io.zeebe.protocol.Protocol.DEFAULT_TOPIC;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.topology.NodeInfo;
import io.zeebe.broker.clustering.base.topology.TopologyPartitionListenerImpl;
import io.zeebe.broker.system.management.topics.FetchCreatedTopicsRequest;
import io.zeebe.broker.system.management.topics.FetchCreatedTopicsResponse;
import io.zeebe.broker.system.workflow.repository.api.management.PushDeploymentRequest;
import io.zeebe.broker.system.workflow.repository.api.management.PushDeploymentResponse;
import io.zeebe.protocol.Protocol;
import io.zeebe.transport.ClientResponse;
import io.zeebe.transport.ClientTransport;
import io.zeebe.util.sched.ActorCondition;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.time.Duration;
import java.util.Iterator;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.Long2ObjectHashMap;
import org.slf4j.Logger;

public class DeploymentDistributor {

  private static final Logger LOG = Loggers.WORKFLOW_REPOSITORY_LOGGER;
  public static final Duration PUSH_REQUEST_TIMEOUT = Duration.ofSeconds(15);
  public static final Duration PARTITION_LEADER_RESOLVE_RETRY = Duration.ofMillis(100);
  public static final Duration FETCH_TOPICS_TIMEOUT = Duration.ofSeconds(15);

  private final PushDeploymentRequest pushDeploymentRequest = new PushDeploymentRequest();
  private final PushDeploymentResponse pushDeploymentResponse = new PushDeploymentResponse();

  private final FetchCreatedTopicsRequest fetchCreatedTopicsRequest =
      new FetchCreatedTopicsRequest();
  private final FetchCreatedTopicsResponse fetchCreatedTopicsResponse =
      new FetchCreatedTopicsResponse();

  private final Long2ObjectHashMap<PendingDeploymentDistribution> pendingDistributions =
      new Long2ObjectHashMap<>();
  private final ActorFuture<Void> partitionsResolved = new CompletableActorFuture<>();

  private final ClientTransport managementApi;
  private final TopologyPartitionListenerImpl partitionListener;
  private final ActorControl actor;
  private final ActorCondition updatePartition;

  private IntArrayList partitionsToDistributeTo;

  public DeploymentDistributor(
      ClientTransport managementApi,
      TopologyPartitionListenerImpl partitionListener,
      ActorControl actor) {
    this.managementApi = managementApi;
    this.partitionListener = partitionListener;
    this.actor = actor;
    this.updatePartition = actor.onCondition("updatePartition", this::fetchTopics);
  }

  public ActorFuture<Void> pushDeployment(long key, long position, DirectBuffer buffer) {
    final ActorFuture<Void> pushedFuture = new CompletableActorFuture<>();

    final PendingDeploymentDistribution pendingDeploymentDistribution =
        new PendingDeploymentDistribution(buffer, position, pushedFuture);
    pendingDistributions.put(key, pendingDeploymentDistribution);

    if (!partitionsResolved.isDone()) {
      final Integer systemPartitionLeaderId = partitionListener.getSystemPartitionLeaderId();
      if (systemPartitionLeaderId == null) {
        partitionListener.addCondition(updatePartition);
      } else {
        fetchTopics();
      }
    }

    actor.runOnCompletion(
        partitionsResolved,
        (v, failure) -> {
          pushDeploymentToPartitions(key);
        });

    return pushedFuture;
  }

  public PendingDeploymentDistribution removePendingDeployment(long key) {
    return pendingDistributions.remove(key);
  }

  private void pushDeploymentToPartitions(long key) {
    if (!partitionsToDistributeTo.isEmpty()) {
      deployOnMultiplePartitions(key);
    } else {
      LOG.trace("No other partitions to distribute deployment.");
      LOG.trace("Deployment finished.");
      pendingDistributions.get(key).complete();
    }
  }

  private void deployOnMultiplePartitions(long key) {
    LOG.trace("Distribute deployment to other partitions.");

    final PendingDeploymentDistribution pendingDeploymentDistribution =
        pendingDistributions.get(key);
    final DirectBuffer directBuffer = pendingDeploymentDistribution.getDeployment();
    pendingDeploymentDistribution.setDistributionCount(partitionsToDistributeTo.size());

    pushDeploymentRequest.reset();
    pushDeploymentRequest.deployment(directBuffer).deploymentKey(key);

    final IntArrayList modifiablePartitionsList = new IntArrayList();
    modifiablePartitionsList.addAll(partitionsToDistributeTo);

    distributeDeployment(modifiablePartitionsList);
  }

  private void distributeDeployment(IntArrayList partitionsToDistribute) {
    final IntArrayList remainingPartitions =
        distributeDeploymentToPartitions(partitionsToDistribute);

    if (remainingPartitions.isEmpty()) {
      LOG.trace("Pushed deployment to all partitions");
      return;
    }

    actor.runDelayed(
        PARTITION_LEADER_RESOLVE_RETRY,
        () -> {
          distributeDeployment(remainingPartitions);
        });
  }

  private IntArrayList distributeDeploymentToPartitions(IntArrayList remainingPartitions) {
    final Int2ObjectHashMap<NodeInfo> currentPartitionLeaders =
        partitionListener.getPartitionLeaders();

    final Iterator<Integer> iterator = remainingPartitions.iterator();
    while (iterator.hasNext()) {
      final Integer partitionId = iterator.next();
      final NodeInfo leader = currentPartitionLeaders.get(partitionId);
      if (leader != null) {
        iterator.remove();
        pushDeploymentToPartition(leader.getNodeId(), partitionId);
      }
    }
    return remainingPartitions;
  }

  private void pushDeploymentToPartition(int partitionLeaderId, int partition) {
    pushDeploymentRequest.partitionId(partition);
    final ActorFuture<ClientResponse> pushResponseFuture =
        managementApi
            .getOutput()
            .sendRequestWithRetry(
                () -> partitionLeaderId,
                (response) -> !pushDeploymentResponse.tryWrap(response),
                pushDeploymentRequest,
                PUSH_REQUEST_TIMEOUT);

    LOG.debug("Deployment pushed to partition {} (node id: {}).", partition, partitionLeaderId);
    actor.runOnCompletion(
        pushResponseFuture,
        (response, throwable) -> {
          if (throwable == null) {
            handlePushResponse(response);
          } else {
            LOG.error(
                "Error on pushing deployment to partition {}. Retry request. ",
                partition,
                throwable);

            final Int2ObjectHashMap<NodeInfo> partitionLeaders =
                partitionListener.getPartitionLeaders();
            final NodeInfo currentLeader = partitionLeaders.get(partition);
            if (currentLeader != null) {
              pushDeploymentToPartition(currentLeader.getNodeId(), partition);
            } else {
              pushDeploymentToPartition(partitionLeaderId, partition);
            }
          }
        });
  }

  private void handlePushResponse(ClientResponse response) {
    pushDeploymentResponse.wrap(response.getResponseBuffer());
    final long deploymentKey = pushDeploymentResponse.deploymentKey();
    final PendingDeploymentDistribution pendingDeploymentDistribution =
        pendingDistributions.get(deploymentKey);

    final long remainingPartitions = pendingDeploymentDistribution.decrementCount();
    if (remainingPartitions == 0) {
      LOG.debug("Deployment pushed to all partitions successfully.");
      pendingDeploymentDistribution.complete();
    } else {
      LOG.trace(
          "Deployment was pushed to partition {} successfully.",
          pushDeploymentResponse.partitionId());
    }
  }

  ////////////////////////////////////////////////
  //////////// topics / partition ids
  ////////////////////////////////////////////////

  private void fetchTopics() {
    final Integer systemPartitionLeaderId = partitionListener.getSystemPartitionLeaderId();
    if (systemPartitionLeaderId != null) {
      partitionListener.removeCondition(updatePartition);

      final ActorFuture<ClientResponse> future =
          managementApi
              .getOutput()
              .sendRequestWithRetry(
                  () -> systemPartitionLeaderId,
                  b -> !fetchCreatedTopicsResponse.tryWrap(b),
                  fetchCreatedTopicsRequest,
                  FETCH_TOPICS_TIMEOUT);

      actor.runOnCompletion(
          future,
          (response, failure) -> {
            if (failure == null) {
              handleFetchCreatedTopicsResponse(response.getResponseBuffer());
            } else {
              LOG.debug("Problem on fetching topics", failure);
            }
          });
    }
  }

  private void handleFetchCreatedTopicsResponse(DirectBuffer response) {
    fetchCreatedTopicsResponse.wrap(response);
    fetchCreatedTopicsResponse
        .getTopics()
        .forEach(
            topic -> {
              if (topic.getTopicName().equals(DEFAULT_TOPIC)) {
                partitionsToDistributeTo = topic.getPartitionIds();
                partitionsToDistributeTo.removeInt(
                    Protocol.DEPLOYMENT_PARTITION); // no need to send push to him self
              }
            });

    if (!partitionsResolved.isDone()) {
      partitionsResolved.complete(null);
    }
  }
}
