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
package io.zeebe.broker.exporter.manager;

import static io.zeebe.broker.exporter.ExporterCommitMessage.POSITION_UNKNOWN;
import static io.zeebe.util.StringUtil.getBytes;

import io.zeebe.logstreams.state.StateController;
import io.zeebe.util.buffer.BufferUtil;
import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import org.rocksdb.RocksDBException;

public class ExporterManagerState extends StateController {
  private final ByteBuffer dbLongBuffer = ByteBuffer.allocate(Long.BYTES);

  public void setPosition(final String exporterId, final long position) throws RocksDBException {
    setPosition(getBytes(exporterId), position);
  }

  public void setPosition(final DirectBuffer id, final long position) throws RocksDBException {
    setPosition(BufferUtil.bufferAsArray(id), position);
  }

  public void setPosition(final byte[] key, final long position) throws RocksDBException {
    ensureIsOpened("setPosition");
    dbLongBuffer.putLong(0, position);
    getDb().put(key, dbLongBuffer.array());
  }

  public long getPosition(final String id) {
    return getPosition(getBytes(id));
  }

  public long getPosition(final DirectBuffer id) {
    return getPosition(BufferUtil.bufferAsArray(id));
  }

  public long getPosition(final byte[] id) {
    ensureIsOpened("getPosition");

    long position = POSITION_UNKNOWN;
    if (tryGet(id, dbLongBuffer.array())) {
      position = dbLongBuffer.getLong(0);
    }

    return position;
  }
}
