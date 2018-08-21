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
package io.zeebe.broker.exporter.stream;

import io.zeebe.broker.exporter.stream.ExporterRecord.ExporterPosition;
import io.zeebe.logstreams.state.StateController;
import io.zeebe.util.LangUtil;
import io.zeebe.util.buffer.BufferUtil;
import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

public class ExporterStreamProcessorState extends StateController {
  private final ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);

  public void setPosition(final String exporterId, final long position) {
    final byte[] key = exporterId.getBytes();
    final DirectBuffer keyBuffer = new UnsafeBuffer(key);

    setPosition(keyBuffer, position);
  }

  public void setPosition(final DirectBuffer exporterId, final long position) {
    final byte[] key = BufferUtil.bufferAsArray(exporterId);
    longBuffer.putLong(0, position);

    try {
      getDb().put(key, longBuffer.array());
    } catch (RocksDBException e) {
      LangUtil.rethrowUnchecked(e);
    }
  }

  public long getPosition(final String exporterId) {
    return getPosition(BufferUtil.wrapString(exporterId));
  }

  public long getPosition(final DirectBuffer exporterId) {
    final byte[] key = BufferUtil.bufferAsArray(exporterId);
    long position = -1;

    try {
      final int bytesRead = getDb().get(key, longBuffer.array());
      if (bytesRead == Long.BYTES) {
        position = longBuffer.getLong(0);
      }
    } catch (RocksDBException e) {
      LangUtil.rethrowUnchecked(e);
    }

    return position;
  }

  public ExporterRecord newExporterRecord() {
    final ExporterRecord record = new ExporterRecord();

    try (final RocksIterator iterator = getDb().newIterator()) {
      for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
        final DirectBuffer wrapper = new UnsafeBuffer(iterator.value());
        final ExporterPosition position = record.getPositionsProperty().add();

        position.getIdProperty().setValue(BufferUtil.wrapArray(iterator.key()));
        position.getPositionProperty().setValue(wrapper.getLong(0));
      }
    }

    return record;
  }
}
