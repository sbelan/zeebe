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
package io.zeebe.client;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.client.api.ZeebeFuture;
import io.zeebe.client.api.commands.BrokerInfo;
import io.zeebe.client.api.commands.PartitionBrokerRole;
import io.zeebe.client.api.commands.PartitionInfo;
import io.zeebe.client.api.events.DeploymentEvent;
import java.net.URISyntaxException;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class ZeebeClientTest {

  @Rule public EmbeddedBrokerRule rule = new EmbeddedBrokerRule();

  private ZeebeClient client;

  @Before
  public void setUp() {
    client = ZeebeClient.newClient();
  }

  @Test
  public void shouldGetHealthCheck() throws InterruptedException {
    Stream.generate(() -> client.newTopologyRequest().send())
        .limit(1000)
        .map(ZeebeFuture::join)
        .forEach(
            response -> {
              assertThat(response).isNotNull();

              final BrokerInfo broker = response.getBrokers().get(0);
              assertThat(broker.getAddress()).isEqualTo("0.0.0.0:26501");
              assertThat(broker.getPartitions().size()).isEqualTo(2);
              final PartitionInfo internalSystem = broker.getPartitions().get(0);
              final PartitionInfo defaultTopic = broker.getPartitions().get(1);

              assertThat(internalSystem.getPartitionId()).isEqualTo(0);
              assertThat(internalSystem.getTopicName()).isEqualTo("internal-system");
              assertThat(internalSystem.getRole()).isEqualTo(PartitionBrokerRole.LEADER);

              assertThat(defaultTopic.getPartitionId()).isEqualTo(1);
              assertThat(defaultTopic.getTopicName()).isEqualTo("default-topic");
              assertThat(defaultTopic.getRole()).isEqualTo(PartitionBrokerRole.LEADER);
            });
  }

  @Test
  public void shouldDeployWorkflow() throws InterruptedException, URISyntaxException {
    final String filePath =
        getClass().getResource("/workflows/demo-process.bpmn").toURI().getPath();

    final DeploymentEvent deploymentResponse =
        client
            .topicClient()
            .workflowClient()
            .newDeployCommand()
            .addResourceFile(filePath)
            .send()
            .join();

    assertThat(deploymentResponse).isNotNull();
  }
}
