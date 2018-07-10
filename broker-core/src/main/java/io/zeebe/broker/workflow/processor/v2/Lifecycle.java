package io.zeebe.broker.workflow.processor.v2;

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.logstreams.processor.EventLifecycleContext;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.intent.Intent;
import io.zeebe.util.sched.ActorControl;

public interface Lifecycle<I extends Enum<I> & Intent, V extends UnpackedObject> {

  void process(RecordWriter recordWriter, TypedRecord<V> record, EventLifecycleContext ct);

  void onPublish(long key, I intent, V valuex);

  // TODO: consolidate with StreamProcessorLifecycleAware
  void onOpen(ActorControl streamProcessorActor);
}
