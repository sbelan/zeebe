package io.zeebe.broker.exporter.processor;

import io.zeebe.exporter.spi.Event;
import io.zeebe.exporter.spi.Exporter;
import io.zeebe.logstreams.log.LogStreamRecordWriter;
import io.zeebe.logstreams.processor.EventLifecycleContext;
import io.zeebe.logstreams.processor.EventProcessor;

public class ExporterEventProcessor implements EventProcessor {
  private final Exporter exporter;
  private Event currentEvent;

  public ExporterEventProcessor(final Exporter exporter) {
    this.exporter = exporter;
  }

  public void wrap(Event event) {
    this.currentEvent = event;
  }

  @Override
  public void processEvent(EventLifecycleContext ctx) {}

  @Override
  public boolean executeSideEffects() {
    if (currentEvent == null) {
      return false;
    }

    exporter.export(currentEvent);
    currentEvent = null;
    return true;
  }

  @Override
  public long writeEvent(LogStreamRecordWriter writer) {
    return 0;
  }

  @Override
  public void updateState() {}
}
