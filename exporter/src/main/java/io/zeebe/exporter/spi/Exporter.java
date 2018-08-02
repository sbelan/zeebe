package io.zeebe.exporter.spi;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public interface Exporter {
  /** @return the configuration for this exporter */
  Configuration getConfiguration();

  /** Starts the exporter. After this returns, the exporter will start receiving events. */
  default Future<Void> start(Context context) {
    return CompletableFuture.completedFuture(null);
  }

  /** Stops the exporter. After this returns, the exporter will not be receiving events anymore. */
  default Future<Void> stop() {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Should export the given events.
   *
   * @return a future which should be completed with the last successfully processed event
   */
  Future<Event> export(Event record);
}
