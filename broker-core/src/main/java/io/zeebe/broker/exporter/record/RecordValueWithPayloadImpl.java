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
package io.zeebe.broker.exporter.record;

import io.zeebe.exporter.record.RecordValueWithPayload;
import io.zeebe.msgpack.UnpackedObject;
import java.io.IOException;
import java.util.Map;
import org.agrona.DirectBuffer;

public abstract class RecordValueWithPayloadImpl extends RecordValueImpl
    implements RecordValueWithPayload {

  private String payload;
  private final DirectBuffer payloadBuffer;

  protected RecordValueWithPayloadImpl(
      final UnpackedObject record, final DirectBuffer payloadBuffer) {
    super(record);
    this.payloadBuffer = payloadBuffer;
  }

  @Override
  public String getPayload() {
    if (payload == null) {
      payload = asString(payloadBuffer);
    }

    return payload;
  }

  @Override
  public Map<String, Object> getPayloadAsMap() throws IOException {
    return asMsgPackMap(payloadBuffer);
  }

  @Override
  public <T> T getPayloadAsType(Class<T> payloadType) throws IOException {
    return asMsgPack(payloadBuffer, payloadType);
  }
}
