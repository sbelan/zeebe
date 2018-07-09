package io.zeebe.broker.workflow.processor.v2;

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.intent.Intent;
import io.zeebe.util.sched.ActorControl;

public interface Lifecycle<I extends Enum<I> & Intent, V extends UnpackedObject> {

  void process(RecordWriter recordWriter, TypedRecord<V> record);

  void onPublish(long key, I intent, V value);

  // TODO: consolidate with StreamProcessorLifecycleAware
  void onOpen(ActorControl streamProcessorActor);
}
