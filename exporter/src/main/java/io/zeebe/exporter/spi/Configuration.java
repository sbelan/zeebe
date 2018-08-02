package io.zeebe.exporter.spi;

import java.time.Duration;

/** */
public interface Configuration {
  /** @return how long to wait before failing the start operation */
  Duration getStartTimeout();

  /** @return how long to wait before failing the stop operation */
  Duration getStopTimeout();

  /** @return how long to wait before failing an export operation */
  Duration getExportTimeout();
}
