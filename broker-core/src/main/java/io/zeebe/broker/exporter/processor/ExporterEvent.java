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
package io.zeebe.broker.exporter.processor;

import static io.zeebe.util.buffer.BufferUtil.toByteBuffer;

import io.zeebe.exporter.spi.Event;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.impl.RecordMetadata;
import java.nio.ByteBuffer;

public class ExporterEvent implements Event {
  private LoggedEvent rawEvent;
  private RecordMetadata metadata = new RecordMetadata();
  private UnpackedObject value = new UnpackedObject();
  private final int partitionId;

  public ExporterEvent(int partitionId) {
    this.partitionId = partitionId;
  }

  public ExporterEvent wrap(final LoggedEvent rawEvent) {
    this.rawEvent = rawEvent;
    this.rawEvent.readMetadata(metadata);
    this.rawEvent.readValue(value);

    return this;
  }

  @Override
  public int getPartitionId() {
    return partitionId;
  }

  @Override
  public long getPosition() {
    return rawEvent.getPosition();
  }

  @Override
  public long getSourceRecordPosition() {
    return rawEvent.getSourceEventPosition();
  }

  @Override
  public long getTimestamp() {
    return rawEvent.getTimestamp();
  }

  @Override
  public long getKey() {
    return rawEvent.getKey();
  }

  @Override
  public short getIntent() {
    return metadata.getIntent().value();
  }

  @Override
  public short getRecordType() {
    return metadata.getRecordType().value();
  }

  @Override
  public short getRejectionType() {
    return metadata.getRejectionType().value();
  }

  @Override
  public ByteBuffer getRejectionReason() {
    return toByteBuffer(metadata.getRejectionReason());
  }

  @Override
  public ByteBuffer getValue() {
    return toByteBuffer(rawEvent.getValueBuffer());
  }

  @Override
  public short getValueType() {
    return metadata.getValueType().value();
  }
}
