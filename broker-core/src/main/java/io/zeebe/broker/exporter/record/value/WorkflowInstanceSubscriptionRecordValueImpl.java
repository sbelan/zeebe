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
import io.zeebe.broker.subscription.message.data.WorkflowInstanceSubscriptionRecord;
import io.zeebe.exporter.record.value.WorkflowInstanceSubscriptionRecordValue;

public class WorkflowInstanceSubscriptionRecordValueImpl extends RecordValueWithPayloadImpl
    implements WorkflowInstanceSubscriptionRecordValue {
  private final WorkflowInstanceSubscriptionRecord record;

  private String messageName;

  public WorkflowInstanceSubscriptionRecordValueImpl(
      final WorkflowInstanceSubscriptionRecord record) {
    super(record, record.getPayload());
    this.record = record;
  }

  @Override
  public String getMessageName() {
    if (messageName == null) {
      messageName = asString(record.getMessageName());
    }

    return messageName;
  }

  @Override
  public long getWorkflowInstanceKey() {
    return record.getWorkflowInstanceKey();
  }

  @Override
  public long getActivityInstanceKey() {
    return record.getActivityInstanceKey();
  }
}
