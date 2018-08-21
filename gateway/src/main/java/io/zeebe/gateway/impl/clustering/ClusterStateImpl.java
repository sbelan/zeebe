/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.gateway.impl.clustering;

import io.zeebe.gateway.api.commands.Topology;
import io.zeebe.transport.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.BiConsumer;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.IntHashSet;

/**
 * Immutable; Important because we hand this between actors. If this is supposed to become mutable,
 * make sure to make copies in the right places.
 */
public class ClusterStateImpl implements ClusterState {
  public static final int NODE_ID_NULL_VALUE = -32;
  public static final int INITIAL_CONTACT_POINT = -64;

  private final Int2IntHashMap topicLeaders = new Int2IntHashMap(NODE_ID_NULL_VALUE);
  private final IntArrayList brokers = new IntArrayList(5, NODE_ID_NULL_VALUE);
  private final Map<String, IntArrayList> partitionsByTopic = new HashMap<>();

  private final Random randomBroker = new Random();

  public ClusterStateImpl(
      final SocketAddress initialContactPoint,
      BiConsumer<Integer, SocketAddress> endpointRegistry) {
    endpointRegistry.accept(INITIAL_CONTACT_POINT, initialContactPoint);
    brokers.add(INITIAL_CONTACT_POINT);
  }

  public ClusterStateImpl(
      Topology topologyDto, BiConsumer<Integer, SocketAddress> endpointRegistry) {
    final Map<String, IntHashSet> partitions = new HashMap<>();

    topologyDto
        .getBrokers()
        .forEach(
            b -> {
              final int nodeId = b.getNodeId();
              endpointRegistry.accept(nodeId, new SocketAddress(b.getHost(), b.getPort()));
              brokers.add(nodeId);

              b.getPartitions()
                  .forEach(
                      p -> {
                        final String topicName = p.getTopicName();
                        final int partitionId = p.getPartitionId();

                        partitions
                            .computeIfAbsent(topicName, t -> new IntHashSet())
                            .add(partitionId);

                        if (p.isLeader()) {
                          topicLeaders.put(partitionId, nodeId);
                        }
                      });
            });

    partitions.forEach(
        (t, p) -> {
          final IntArrayList partitionsList = new IntArrayList();
          final IntHashSet.IntIterator iterator = p.iterator();
          while (iterator.hasNext()) {
            partitionsList.add(iterator.nextValue());
          }

          partitionsByTopic.put(t, partitionsList);
        });
  }

  @Override
  public int getLeaderForPartition(int partition) {
    return topicLeaders.get(partition);
  }

  @Override
  public int getRandomBroker() {
    if (!brokers.isEmpty()) {
      final int nextBroker = randomBroker.nextInt(brokers.size());
      return brokers.getInt(nextBroker);
    } else {
      throw new RuntimeException("Unable to select random broker from empty list");
    }
  }

  @Override
  public IntArrayList getPartitionsOfTopic(String topic) {
    return partitionsByTopic.get(topic);
  }

  public int getPartition(String topic, int offset) {
    final IntArrayList partitions = getPartitionsOfTopic(topic);

    if (partitions != null && !partitions.isEmpty()) {
      return partitions.getInt(offset % partitions.size());
    } else {
      return -1;
    }
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("ClusterState [topicLeaders=");
    builder.append(topicLeaders);
    builder.append(", brokers=");
    builder.append(brokers);
    builder.append("]");
    return builder.toString();
  }
}
