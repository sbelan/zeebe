package io.zeebe.broker.exporter.processor;

import io.zeebe.exporter.spi.Event;
import io.zeebe.logstreams.log.LoggedEvent;

import java.nio.ByteBuffer;

public class ExporterEvent implements Event {
  private LoggedEvent event;

  public void wrap(final LoggedEvent event) {
    this.event = event;
  }

  @Override
  public int getPartitionId() {
    return 0;
  }

  @Override
  public long getPosition() {
    return 0;
  }

  @Override
  public long getSourceRecordPosition() {
    return 0;
  }

  @Override
  public long getTimestamp() {
    return 0;
  }

  @Override
  public long getKey() {
    return 0;
  }

  @Override
  public short getIntent() {
    return 0;
  }

  @Override
  public short getRecordType() {
    return 0;
  }

  @Override
  public short getRejectionType() {
    return 0;
  }

  @Override
  public ByteBuffer getRejectionReason() {
    return null;
  }

  @Override
  public ByteBuffer getValue() {
    return null;
  }

  @Override
  public short getValueType() {
    return 0;
  }
}
