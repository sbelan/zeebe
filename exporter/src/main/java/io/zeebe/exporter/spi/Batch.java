package io.zeebe.exporter.spi;

import java.util.stream.Stream;

public interface Batch extends Iterable<Event> {
  int size();

  void commit();

  Stream<Event> stream();
}
