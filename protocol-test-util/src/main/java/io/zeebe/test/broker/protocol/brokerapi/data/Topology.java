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
package io.zeebe.test.broker.protocol.brokerapi.data;

import static io.zeebe.test.broker.protocol.brokerapi.data.BrokerPartitionState.FOLLOWER_STATE;
import static io.zeebe.test.broker.protocol.brokerapi.data.BrokerPartitionState.LEADER_STATE;

import io.zeebe.test.broker.protocol.brokerapi.StubBrokerRule;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Topology {

  protected Map<Integer, TopologyBroker> brokers = new HashMap<>();

  public Topology() {}

  public Topology(Topology other) {
    this.brokers = new HashMap<>(other.brokers);
  }

  private TopologyBroker getBroker(int nodeId, String host, int port) {
    TopologyBroker topologyBroker = brokers.get(nodeId);
    if (topologyBroker == null) {
      topologyBroker = new TopologyBroker(nodeId, host, port);
      brokers.put(nodeId, topologyBroker);
    }
    return topologyBroker;
  }

  public Topology addLeader(int nodeId, String host, int port, String topic, int partition) {
    getBroker(nodeId, host, port)
        .addPartition(new BrokerPartitionState(LEADER_STATE, topic, partition));

    return this;
  }

  public Topology addFollower(int nodeId, String host, int port, String topic, int partition) {
    getBroker(nodeId, host, port)
        .addPartition(new BrokerPartitionState(FOLLOWER_STATE, topic, partition));

    return this;
  }

  public Topology addLeader(StubBrokerRule broker, String topic, int partition) {
    return addLeader(broker.getNodeId(), broker.getHost(), broker.getPort(), topic, partition);
  }

  public Set<TopologyBroker> getBrokers() {
    return new HashSet<>(brokers.values());
  }

  @Override
  public String toString() {
    return "Topology{" + "brokers=" + brokers.values() + '}';
  }
}
