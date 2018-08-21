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
package io.zeebe.broker.exporter.record.value;

import io.zeebe.broker.exporter.record.RecordValueImpl;
import io.zeebe.broker.exporter.record.value.deployment.DeployedWorkflowImpl;
import io.zeebe.broker.exporter.record.value.deployment.DeploymentResourceImpl;
import io.zeebe.broker.system.workflow.repository.data.DeploymentRecord;
import io.zeebe.exporter.record.value.DeploymentRecordValue;
import io.zeebe.exporter.record.value.deployment.DeployedWorkflow;
import io.zeebe.exporter.record.value.deployment.DeploymentResource;
import java.util.ArrayList;
import java.util.List;

public class DeploymentRecordValueImpl extends RecordValueImpl implements DeploymentRecordValue {
  private final DeploymentRecord record;
  private List<DeployedWorkflow> deployedWorkflows;
  private List<DeploymentResource> resources;

  public DeploymentRecordValueImpl(final DeploymentRecord record) {
    super(record);
    this.record = record;
  }

  @Override
  public List<DeploymentResource> getResources() {
    if (resources == null) {
      resources = new ArrayList<>();
      record.resources().forEach(r -> resources.add(new DeploymentResourceImpl(r)));
    }

    return resources;
  }

  @Override
  public List<DeployedWorkflow> getDeployedWorkflows() {
    if (deployedWorkflows == null) {
      deployedWorkflows = new ArrayList<>();
      record.deployedWorkflows().forEach(d -> deployedWorkflows.add(new DeployedWorkflowImpl(d)));
    }

    return deployedWorkflows;
  }
}
