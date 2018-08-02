package io.zeebe.exporter.spi;

import java.nio.ByteBuffer;

public interface Event {
  int getPartitionId();

  long getPosition();

  long getSourceRecordPosition();

  long getTimestamp();

  long getKey();

  short getIntent();

  short getRecordType();

  short getRejectionType();

  ByteBuffer getRejectionReason();

  ByteBuffer getValue();

  short getValueType();
}
