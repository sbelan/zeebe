package io.zeebe.exporter.spi;

import java.time.Duration;

public interface Configuration {
  /** @return maximum number of events to include in a batch */
  int getBatchSize();

  /** @return how long to wait to flush a partially-filled batch */
  Duration getBatchFlushTimeout();
}
