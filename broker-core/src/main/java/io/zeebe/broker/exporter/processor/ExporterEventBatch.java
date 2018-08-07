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

import io.zeebe.exporter.spi.Batch;
import io.zeebe.exporter.spi.Event;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public class ExporterEventBatch implements Batch {
  private final int limit;
  private final List<Event> events;

  public ExporterEventBatch(final int limit) {
    this.limit = limit;
    this.events = new ArrayList<>(limit);
  }

  @Override
  public int size() {
    return events.size();
  }

  @Override
  public void commit() {
    // send ack!

    // reset
    events.clear();
  }

  @Override
  public Stream<Event> stream() {
    return events.stream();
  }

  @Override
  @Nonnull
  public Iterator<Event> iterator() {
    return events.iterator();
  }

  public boolean add(final Event event) {
    if (size() < limit - 1) {
      events.add(event);
      return true;
    }

    return false;
  }
}
