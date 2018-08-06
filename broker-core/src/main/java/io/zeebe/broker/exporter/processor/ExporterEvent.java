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

import io.zeebe.exporter.spi.Event;
import io.zeebe.logstreams.log.LoggedEvent;
import java.nio.ByteBuffer;

public class ExporterEvent implements Event {
  private LoggedEvent event;

  public void wrap(final LoggedEvent event) {
    this.event = event;
  }

  @Override
  public int getPartitionId() {
    return 0;
  }

  @Override
  public long getPosition() {
    return 0;
  }

  @Override
  public long getSourceRecordPosition() {
    return 0;
  }

  @Override
  public long getTimestamp() {
    return 0;
  }

  @Override
  public long getKey() {
    return 0;
  }

  @Override
  public short getIntent() {
    return 0;
  }

  @Override
  public short getRecordType() {
    return 0;
  }

  @Override
  public short getRejectionType() {
    return 0;
  }

  @Override
  public ByteBuffer getRejectionReason() {
    return null;
  }

  @Override
  public ByteBuffer getValue() {
    return null;
  }

  @Override
  public short getValueType() {
    return 0;
  }
}
