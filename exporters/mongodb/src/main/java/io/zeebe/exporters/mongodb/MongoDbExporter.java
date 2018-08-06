package io.zeebe.exporters.mongodb;

import io.zeebe.exporter.spi.Context;
import io.zeebe.exporter.spi.Event;
import io.zeebe.exporter.spi.Exporter;

import java.util.concurrent.Future;

public class MongoDbExporter implements Exporter {

  @Override
  public Configuration getConfiguration() {
    return null;
  }

  @Override
  public Future<Void> start(Context context) {
    return null;
  }

  @Override
  public Future<Void> stop() {
    return null;
  }

  @Override
  public Future<Event> export(Event record) {
    return null;
  }
}
