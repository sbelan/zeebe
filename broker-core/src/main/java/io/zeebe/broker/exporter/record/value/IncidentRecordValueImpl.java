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
import io.zeebe.broker.incident.data.IncidentRecord;
import io.zeebe.exporter.record.value.IncidentRecordValue;
import io.zeebe.util.buffer.BufferUtil;

public class IncidentRecordValueImpl extends RecordValueWithPayloadImpl
    implements IncidentRecordValue {
  private final IncidentRecord record;

  private String errorMessage;
  private String bpmnProcessId;
  private String activityId;

  public IncidentRecordValueImpl(final IncidentRecord record) {
    super(record, record.getPayload());
    this.record = record;
  }

  @Override
  public String getErrorType() {
    return record.getErrorType().name();
  }

  @Override
  public String getErrorMessage() {
    if (errorMessage == null) {
      errorMessage = BufferUtil.bufferAsString(record.getErrorMessage());
    }

    return errorMessage;
  }

  @Override
  public String getBpmnProcessId() {
    if (bpmnProcessId == null) {
      bpmnProcessId = BufferUtil.bufferAsString(record.getBpmnProcessId());
    }

    return bpmnProcessId;
  }

  @Override
  public long getWorkflowInstanceKey() {
    return 0;
  }

  @Override
  public String getActivityId() {
    if (activityId == null) {
      activityId = BufferUtil.bufferAsString(record.getActivityId());
    }

    return activityId;
  }

  @Override
  public long getActivityInstanceKey() {
    return record.getActivityInstanceKey();
  }

  @Override
  public long getJobKey() {
    return record.getJobKey();
  }
}
