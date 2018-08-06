package io.zeebe.broker.exporter.processor;

import io.zeebe.exporter.spi.Exporter;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.logstreams.processor.EventProcessor;
import io.zeebe.logstreams.processor.StreamProcessor;
import io.zeebe.logstreams.spi.SnapshotSupport;

public class ExporterStreamProcessor implements StreamProcessor {
  private final ExporterEventProcessor eventProcessor;
  private final Exporter exporter;

  public ExporterStreamProcessor(final Exporter exporter) {
    this.exporter = exporter;
    this.eventProcessor = new ExporterEventProcessor(exporter);
  }

  @Override
  public SnapshotSupport getStateResource() {
    return null;
  }

  @Override
  public EventProcessor onEvent(LoggedEvent event) {
    // eventProcessor.wrap(event);
    return eventProcessor;
  }

  @Override
  public void onClose() {}
}
