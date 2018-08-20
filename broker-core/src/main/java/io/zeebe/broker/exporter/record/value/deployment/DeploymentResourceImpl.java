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

import io.zeebe.broker.system.workflow.repository.data.DeploymentResource;
import io.zeebe.exporter.record.value.deployment.DeploymentResourceType;
import io.zeebe.util.buffer.BufferUtil;

public class DeploymentResourceImpl
    implements io.zeebe.exporter.record.value.deployment.DeploymentResource {
  private final DeploymentResource resource;

  private byte[] contents;
  private DeploymentResourceType type;
  private String name;

  public DeploymentResourceImpl(final DeploymentResource resource) {
    this.resource = resource;
  }

  @Override
  public byte[] getResource() {
    if (contents == null) {
      contents = BufferUtil.bufferAsArray(resource.getResource());
    }

    return contents;
  }

  @Override
  public DeploymentResourceType getResourceType() {
    if (type == null) {
      switch (resource.getResourceType()) {
        case BPMN_XML:
          type = DeploymentResourceType.BPMN_XML;
          break;
        case YAML_WORKFLOW:
          type = DeploymentResourceType.YAML_WORKFLOW;
          break;
        default:
          throw new IllegalStateException(
              String.format("unknown type %s", resource.getResourceType()));
      }
    }

    return type;
  }

  @Override
  public String getResourceName() {
    if (name == null) {
      name = BufferUtil.bufferAsString(resource.getResourceName());
    }

    return name;
  }
}
