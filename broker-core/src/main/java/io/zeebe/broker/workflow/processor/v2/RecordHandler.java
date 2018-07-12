package io.zeebe.broker.workflow.processor.v2;

import io.zeebe.broker.logstreams.processor.StreamProcessorLifecycleAware;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.logstreams.processor.EventLifecycleContext;
import io.zeebe.msgpack.UnpackedObject;

public interface RecordHandler<T extends UnpackedObject> extends StreamProcessorLifecycleAware {

  void handle(RecordWriter recordWriter, TypedRecord<T> record, EventLifecycleContext ctx);
}
