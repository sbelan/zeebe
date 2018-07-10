package io.zeebe.broker.workflow.processor.v2;

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.msgpack.UnpackedObject;

public interface RecordHandler<T extends UnpackedObject> {

  void handle(RecordWriter recordWriter, TypedRecord<T> record);
}
