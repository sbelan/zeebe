package io.zeebe.exporter;

import io.zeebe.exporter.spi.Configuration;
import io.zeebe.exporter.spi.Context;
import io.zeebe.exporter.spi.Event;
import io.zeebe.exporter.spi.Exporter;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class LogExporter implements Exporter {
  private static final Future<Void> COMPLETED = CompletableFuture.completedFuture(null);
  private static final String SEPARATOR = String.join("", Collections.nCopies(80, "="));
  private Logger logger;

  @Override
  public Configuration getConfiguration() {
    return null;
  }

  @Override
  public Future<Void> start(Context context) {
    logger = context.getLogger();
    log("Starting...");
    return COMPLETED;
  }

  @Override
  public Future<Void> stop() {
    log("Stopping...");
    return COMPLETED;
  }

  @Override
  public Future<Event> export(Event event) {
    log(event.toString());
    return CompletableFuture.completedFuture(event);
  }

  private void log(final String message) {
    logger.info("{}\n{}\n{}", SEPARATOR, message, SEPARATOR);
  }

  private static class Config implements Configuration {
    private static final Duration INDEFINITELY = Duration.ZERO;

    @Override
    public Duration getStartTimeout() {
      return INDEFINITELY;
    }

    @Override
    public Duration getStopTimeout() {
      return INDEFINITELY;
    }

    @Override
    public Duration getExportTimeout() {
      return INDEFINITELY;
    }
  }
}
