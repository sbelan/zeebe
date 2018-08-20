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

import io.zeebe.broker.exporter.record.RecordValueWithPayloadImpl;
import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.exporter.record.value.WorkflowInstanceRecordValue;

public class WorkflowInstanceRecordValueImpl extends RecordValueWithPayloadImpl
    implements WorkflowInstanceRecordValue {
  private final WorkflowInstanceRecord record;

  private String bpmnProcessId;
  private String activityId;

  public WorkflowInstanceRecordValueImpl(final WorkflowInstanceRecord record) {
    super(record, record.getPayload());
    this.record = record;
  }

  @Override
  public String getBpmnProcessId() {
    if (bpmnProcessId == null) {
      bpmnProcessId = asString(record.getBpmnProcessId());
    }

    return bpmnProcessId;
  }

  @Override
  public String getActivityId() {
    if (activityId == null) {
      activityId = asString(record.getActivityId());
    }

    return activityId;
  }

  @Override
  public int getVersion() {
    return record.getVersion();
  }

  @Override
  public long getWorkflowKey() {
    return record.getWorkflowKey();
  }

  @Override
  public long getWorkflowInstanceKey() {
    return record.getWorkflowInstanceKey();
  }
}
