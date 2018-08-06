package io.zeebe.exporter.spi;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public interface Exporter {
  /**
   * Starts the exporter. After this returns, the exporter will start receiving events.
   *
   * @return the configuration to use for this exporter
   */
  default Future<Configuration> start(Context context) {
    return CompletableFuture.completedFuture(null);
  }

  /** Stops the exporter. After this returns, the exporter will not be receiving events anymore. */
  default Future<Void> stop() {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Should export the given batch of events. The user should call `batch.commit()` to mark the
   * batch as exported.
   *
   * @param batch batch to export
   */
  void exporter(Batch batch);
}
