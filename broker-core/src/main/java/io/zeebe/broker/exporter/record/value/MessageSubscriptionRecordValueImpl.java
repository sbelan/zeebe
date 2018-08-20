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
import io.zeebe.broker.subscription.message.data.MessageSubscriptionRecord;
import io.zeebe.exporter.record.value.MessageSubscriptionRecordValue;

public class MessageSubscriptionRecordValueImpl extends RecordValueImpl
    implements MessageSubscriptionRecordValue {
  private final MessageSubscriptionRecord record;

  private String messageName;
  private String correlationKey;

  public MessageSubscriptionRecordValueImpl(final MessageSubscriptionRecord record) {
    super(record);
    this.record = record;
  }

  @Override
  public int getWorkflowInstancePartitionId() {
    return record.getWorkflowInstancePartitionId();
  }

  @Override
  public long getWorkflowInstanceKey() {
    return record.getWorkflowInstanceKey();
  }

  @Override
  public long getActivityInstanceKey() {
    return record.getActivityInstanceKey();
  }

  @Override
  public String getMessageName() {
    if (messageName == null) {
      messageName = asString(record.getMessageName());
    }

    return messageName;
  }

  @Override
  public String getCorrelationKey() {
    if (correlationKey == null) {
      correlationKey = asString(record.getCorrelationKey());
    }

    return correlationKey;
  }
}
