package io.zeebe.broker.workflow.processor.v2;

import java.util.HashMap;
import java.util.Map;
import io.zeebe.broker.logstreams.processor.TypedBatchWriter;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.intent.Intent;

public class RecordWriter {

  private Map<Class<?>, Lifecycle<?, ?>> lifecycles = new HashMap<>();
  private final KeyGenerator keyGenerator; // TODO: must be part of snapshotted state

  private final TypedStreamWriter streamWriter;
  private final TypedResponseWriter responseWriter;

  private TypedBatchWriter batchWriter;
  // TODO: Must be able to stage the response


  public void publishEvent(Intent intent, UnpackedObject value)
  {
    publishEvent(keyGenerator.nextKey(), intent, value);
  }

  public void reset()
  {
    batchWriter = streamWriter.newBatch();
  }

  public void publishCommand(Intent intent, UnpackedObject value)
  {
    // TODO: implement
  }

  public void publishEvent(long key, Intent intent, UnpackedObject value)
  {
    batchWriter.addFollowUpEvent(key, intent, value);

    final Lifecycle lifecycle = lifecycles.get(value.getClass());

    lifecycle.onPublish(key, (Enum) intent, value);
  }

  public void sendAccept(TypedRecord<?> record, Intent intent)
  {
    // TODO: set source position accordingly if record is event or command
  }

  public void sendReject(TypedRecord<?> record, RejectionType rejectionType, String rejectionReason)
  {

  }

  /**
   * Performs #writeEvents
   */
  public long flushEvents()
  {
    return batchWriter.write();
  }

  public boolean flushSideEffects()
  {
    // TODO: flush response here
    return true;
  }

}
