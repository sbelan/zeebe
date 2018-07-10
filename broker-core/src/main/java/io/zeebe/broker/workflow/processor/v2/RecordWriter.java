package io.zeebe.broker.workflow.processor.v2;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import io.zeebe.broker.logstreams.processor.TypedBatchWriter;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.intent.Intent;

public class RecordWriter {

  private Map<Class<?>, Lifecycle<?, ?>> lifecycles = new HashMap<>();
  private final KeyGenerator keyGenerator; // TODO: must be part of snapshotted state

  private final TypedStreamWriter streamWriter;
  private final ResponseWriter responseWriter;

  private TypedBatchWriter batchWriter;
  // TODO: Must be able to stage the response

  public RecordWriter(TypedStreamWriter streamWriter, ResponseWriter responseWriter)
  {
    this.keyGenerator = new KeyGenerator();
    this.streamWriter = streamWriter;
    this.responseWriter = responseWriter;
  }


  public void reset()
  {
    batchWriter = streamWriter.newBatch();
  }

  public void publishCommand(Intent intent, UnpackedObject value)
  {
    publishRecord(-1, intent, value, RecordType.COMMAND);
  }

  public void publishEvent(long key, Intent intent, UnpackedObject value)
  {
    publishRecord(key, intent, value, RecordType.EVENT);
  }

  public void publishEvent(Intent intent, UnpackedObject value)
  {
    publishEvent(keyGenerator.nextKey(), intent, value);
  }

  private void publishRecord(long key, Intent intent, UnpackedObject value, RecordType recordType)
  {
    batchWriter.addRecord(recordType, key, intent, value);

    final Lifecycle lifecycle = lifecycles.get(value.getClass());

    lifecycle.onPublish(key, (Enum) intent, value);
  }

  public void sendAccept(TypedRecord<?> record, Intent intent)
  {
    responseWriter.sendAccept(record, intent);
  }

  public void sendReject(TypedRecord<?> record, RejectionType rejectionType, String rejectionReason)
  {
    responseWriter.sendReject(record, rejectionType, rejectionReason);
  }

  /**
   * Performs #writeEvents
   */
  public long flushEvents()
  {
    return batchWriter.write();
  }

  /**
   * #executeSideeffects
   */
  public boolean flushSideEffects()
  {
    return responseWriter.flush();
  }

}
