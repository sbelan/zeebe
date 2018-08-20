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
package io.zeebe.broker.exporter.record.value.deployment;

import io.zeebe.broker.system.workflow.repository.data.DeployedWorkflow;
import io.zeebe.util.buffer.BufferUtil;

public class DeployedWorkflowImpl
    implements io.zeebe.exporter.record.value.deployment.DeployedWorkflow {
  private final DeployedWorkflow workflow;

  private String bpmnProcessId;
  private String resourceName;

  public DeployedWorkflowImpl(final DeployedWorkflow workflow) {
    this.workflow = workflow;
  }

  @Override
  public String getBpmnProcessId() {
    if (bpmnProcessId == null) {
      bpmnProcessId = BufferUtil.bufferAsString(workflow.getBpmnProcessId());
    }

    return bpmnProcessId;
  }

  @Override
  public int getVersion() {
    return workflow.getVersion();
  }

  @Override
  public long getWorkflowKey() {
    return workflow.getKey();
  }

  @Override
  public String getResourceName() {
    if (resourceName == null) {
      resourceName = BufferUtil.bufferAsString(workflow.getResourceName());
    }

    return resourceName;
  }
}
