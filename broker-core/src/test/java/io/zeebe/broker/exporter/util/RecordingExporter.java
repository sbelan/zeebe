package io.zeebe.broker.exporter.util;

import io.zeebe.exporter.context.Context;
import io.zeebe.exporter.context.Controller;
import io.zeebe.exporter.record.Record;
import java.util.ArrayList;
import java.util.List;

public class RecordingExporter extends ControlledTestExporter {
  private boolean wasValidated = false;
  private boolean wasOpened = false;
  private boolean wasConfigured = false;
  private boolean wasClosed = false;

  private final List<Record> exportedRecords = new ArrayList<>();

  public boolean wasValidated() {
    return wasValidated;
  }

  public boolean wasOpened() {
    return wasOpened;
  }

  public boolean wasConfigured() {
    return wasConfigured;
  }

  public boolean wasClosed() {
    return wasClosed;
  }

  public List<Record> getExportedRecords() {
    return exportedRecords;
  }

  @Override
  public void configure(Context context) {
    wasConfigured = true;
    super.configure(context);
  }

  @Override
  public boolean validate(Context context) throws Exception {
    wasValidated = true;
    return super.validate(context);
  }

  @Override
  public void open(Controller controller) {
    wasOpened = true;
    super.open(controller);
  }

  @Override
  public void close() {
    wasClosed = true;
    super.close();
  }

  @Override
  public void export(Record record) {
    exportedRecords.add(record);
    super.export(record);
  }
}
