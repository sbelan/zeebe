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
package io.zeebe.client.impl;

import io.zeebe.client.api.commands.DeploymentResource;
import io.zeebe.client.api.commands.Workflow;
import io.zeebe.client.api.events.DeploymentEvent;
import io.zeebe.client.api.events.DeploymentState;
import io.zeebe.client.api.record.RecordMetadata;
import io.zeebe.gateway.protocol.GatewayOuterClass.DeployWorkflowResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.WorkflowInfoResponse;
import java.util.ArrayList;
import java.util.List;

public class DeploymentEventImpl implements DeploymentEvent {

  private final List<Workflow> deployedWorkflows;

  DeploymentEventImpl(final DeployWorkflowResponse response) {
    this.deployedWorkflows = new ArrayList<>();

    for (final WorkflowInfoResponse wf : response.getWorkflowsList()) {
      final Workflow workflow =
          new WorkflowImpl(
              wf.getBpmnProcessId(), wf.getVersion(), wf.getWorkflowKey(), wf.getResourceName());
      deployedWorkflows.add(workflow);
    }
  }

  @Override
  public DeploymentState getState() {
    return DeploymentState.CREATED;
  }

  @Override
  public List<Workflow> getDeployedWorkflows() {
    return deployedWorkflows;
  }

  @Override
  public String getDeploymentTopic() {
    return "default-topic";
  }

  @Override
  public List<DeploymentResource> getResources() {
    return null;
  }

  @Override
  public RecordMetadata getMetadata() {
    return null;
  }

  @Override
  public String toJson() {
    return null;
  }

  @Override
  public long getKey() {
    return 0;
  }
}
