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
package io.zeebe.gateway;

import io.zeebe.gateway.api.clients.WorkflowClient;
import io.zeebe.gateway.api.commands.DeployWorkflowCommandStep1;
import io.zeebe.gateway.api.commands.DeployWorkflowCommandStep1.DeployWorkflowCommandBuilderStep2;
import io.zeebe.gateway.api.commands.Topology;
import io.zeebe.gateway.api.events.DeploymentEvent;
import io.zeebe.gateway.protocol.GatewayOuterClass.DeployWorkflowRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.HealthRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.WorkflowInfoRequest;
import io.zeebe.util.sched.future.ActorFuture;
import java.util.List;

public class ClusterClient {

  private final ZeebeClient client;
  private final WorkflowClient workflowClient;

  public ClusterClient(final ZeebeClient client) {
    this.client = client;
    this.workflowClient = client.topicClient().workflowClient();
  }

  public ActorFuture<Topology> sendHealthRequest(final HealthRequest request) {
    return client.newTopologyRequest().send();
  }

  public ActorFuture<DeploymentEvent> sendDeployWorkflowRequest(
      final DeployWorkflowRequest request) {
    final DeployWorkflowCommandStep1 deployCmd = this.workflowClient.newDeployCommand();

    final List<WorkflowInfoRequest> workflows = request.getWorkflowsList();

    DeployWorkflowCommandBuilderStep2 builder =
        deployCmd.addResourceBytes(
            workflows.get(0).getDefinition().toByteArray(), workflows.get(0).getName());

    for (int i = 1; i < workflows.size(); i++) {
      builder =
          builder.addResourceBytes(
              workflows.get(i).getDefinition().toByteArray(), workflows.get(i).getName());
    }

    return builder.send();
  }
}
