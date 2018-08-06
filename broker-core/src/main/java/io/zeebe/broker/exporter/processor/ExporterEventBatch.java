package io.zeebe.broker.exporter.processor;

import io.zeebe.exporter.spi.Batch;
import io.zeebe.exporter.spi.Event;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

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
