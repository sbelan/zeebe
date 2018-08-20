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
import io.zeebe.broker.subscription.message.data.MessageRecord;
import io.zeebe.exporter.record.value.MessageRecordValue;

public class MessageRecordValueImpl extends RecordValueWithPayloadImpl
    implements MessageRecordValue {
  private final MessageRecord record;

  private String name;
  private String messageId;
  private String correlationKey;

  public MessageRecordValueImpl(final MessageRecord record) {
    super(record, record.getPayload());
    this.record = record;
  }

  @Override
  public String getName() {
    if (name == null) {
      name = asString(record.getName());
    }
    return null;
  }

  @Override
  public String getCorrelationKey() {
    return null;
  }

  @Override
  public String getMessageId() {
    return null;
  }
}
