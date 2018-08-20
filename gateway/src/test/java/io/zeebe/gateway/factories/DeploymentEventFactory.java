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
package io.zeebe.gateway.factories;

import io.zeebe.gateway.api.commands.DeploymentResource;
import io.zeebe.gateway.api.commands.Workflow;
import io.zeebe.gateway.api.events.DeploymentEvent;
import io.zeebe.gateway.api.events.DeploymentState;
import io.zeebe.gateway.api.record.RecordMetadata;
import io.zeebe.gateway.impl.event.WorkflowImpl;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class DeploymentEventFactory implements TestFactory<DeploymentEvent> {

  @Override
  public DeploymentEvent getFixture() {
    return new DeploymentEvent() {
      List<Workflow> deployedWorkflows = new LinkedList<>();

      @Override
      public DeploymentState getState() {
        return DeploymentState.CREATED;
      }

      @Override
      public List<Workflow> getDeployedWorkflows() {
        final WorkflowImpl workflow = new WorkflowImpl();
        workflow.setBpmnProcessId("bpmnProcessId");
        workflow.setResourceName("process.bpmn");
        workflow.setVersion(5);
        workflow.setWorkflowKey(123456789);
        deployedWorkflows.add(workflow);

        return deployedWorkflows;
      }

      @Override
      public String getDeploymentTopic() {
        return "default-topic";
      }

      @Override
      public List<DeploymentResource> getResources() {
        return new ArrayList<>();
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
    };
  }
}
