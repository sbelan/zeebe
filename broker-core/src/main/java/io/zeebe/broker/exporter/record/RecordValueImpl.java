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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.zeebe.exporter.record.RecordValue;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.util.buffer.BufferUtil;
import java.io.IOException;
import java.util.Map;
import org.agrona.DirectBuffer;
import org.agrona.io.DirectBufferInputStream;
import org.msgpack.jackson.dataformat.MessagePackFactory;

public abstract class RecordValueImpl implements RecordValue {
  private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE =
      new TypeReference<Map<String, Object>>() {};
  protected static final ObjectReader MSG_PACK_READER =
      new ObjectMapper(new MessagePackFactory()).reader();

  protected DirectBufferInputStream msgPackInputStream = new DirectBufferInputStream();

  private final UnpackedObject record;

  protected RecordValueImpl(final UnpackedObject record) {
    this.record = record;
  }

  @Override
  public String toJson() {
    final StringBuilder builder = new StringBuilder();
    record.writeJSON(builder);
    return builder.toString();
  }

  protected String asString(final DirectBuffer buffer) {
    return BufferUtil.bufferAsString(buffer);
  }

  protected <T> T asMsgPack(final DirectBuffer buffer, Class<T> type) throws IOException {
    msgPackInputStream.wrap(buffer);
    return MSG_PACK_READER.forType(type).readValue(msgPackInputStream);
  }

  protected Map<String, Object> asMsgPackMap(final DirectBuffer buffer) throws IOException {
    msgPackInputStream.wrap(buffer);
    return MSG_PACK_READER.forType(MAP_TYPE_REFERENCE).readValue(msgPackInputStream);
  }
}
