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
package io.zeebe.broker.exporter.record.value.job;

import io.zeebe.broker.job.data.JobHeaders;
import io.zeebe.exporter.record.value.job.Headers;
import io.zeebe.util.buffer.BufferUtil;

public class HeadersImpl implements Headers {
  private final JobHeaders headers;

  private String bpmnProcessId;
  private String activityId;

  public HeadersImpl(final JobHeaders headers) {
    this.headers = headers;
  }

  @Override
  public String getActivityId() {
    if (activityId == null) {
      activityId = BufferUtil.bufferAsString(headers.getActivityId());
    }

    return activityId;
  }

  @Override
  public long getActivityInstanceKey() {
    return headers.getActivityInstanceKey();
  }

  @Override
  public String getBpmnProcessId() {
    if (bpmnProcessId == null) {
      bpmnProcessId = BufferUtil.bufferAsString(headers.getBpmnProcessId());
    }

    return bpmnProcessId;
  }

  @Override
  public int getWorkflowDefinitionVersion() {
    return headers.getWorkflowDefinitionVersion();
  }

  @Override
  public long getWorkflowInstanceKey() {
    return headers.getWorkflowInstanceKey();
  }

  @Override
  public long getWorkflowKey() {
    return headers.getWorkflowKey();
  }
}
