package io.zeebe.broker.exporter;

import io.zeebe.exporter.spi.*;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class LogExporter implements Exporter, Configuration {
  private static final Future<Void> FINISHED = CompletableFuture.completedFuture(null);
  private static final int BATCH_SIZE = 100;
  private static final Duration BATCH_FLUSH_TIMEOUT = Duration.ofSeconds(1);

  private Logger logger;
  private String id;

  @Override
  public Future<Configuration> start(Context context) {
    id = context.getId();
    logger = context.getLogger();
    logger.info("Starting exporter {}", id);

    return CompletableFuture.completedFuture(this);
  }

  @Override
  public Future<Void> stop() {
    logger.info("Stopping exporter {}", id);

    return FINISHED;
  }

  @Override
  public void exporter(Batch batch) {
    for (Event event : batch) {
      logger.info("Processed {}", event.toString());
    }

    batch.commit();
  }

  @Override
  public int getBatchSize() {
    return BATCH_SIZE;
  }

  @Override
  public Duration getBatchFlushTimeout() {
    return BATCH_FLUSH_TIMEOUT;
  }
}
