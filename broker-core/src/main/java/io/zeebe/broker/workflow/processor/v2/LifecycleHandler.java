package io.zeebe.broker.workflow.processor.v2;

import io.zeebe.broker.logstreams.processor.TypedBatchWriter;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.intent.Intent;

// TODO: naming
public class LifecycleHandler<I extends Enum<I> & Intent, V extends UnpackedObject> {

  /*
   * purposes:
   *   - update state (activity instance and workflow instance)
   *   - send responses (not clear)
   */
  private final Lifecycle<I, V> lifecycle;
  private final KeyGenerator keyGenerator; // TODO: must be part of snapshotted state

  private final TypedStreamWriter streamWriter;
  private final TypedResponseWriter responseWriter;

  private TypedRecord<V> currentRecord;

  private TypedBatchWriter batchWriter;
  // TODO: Must be able to stage the response

  public LifecycleHandler(Lifecycle<I, V> lifecycle, TypedStreamWriter streamWriter, TypedResponseWriter responseWriter)
  {
    this.lifecycle = lifecycle;
    this.streamWriter = streamWriter;
    this.responseWriter = responseWriter;
    this.keyGenerator = new KeyGenerator();
  }

  // TODO: responses are sent as part of a lifecycle listener

//  // TODO: always propagate request context if no response is sent
//
//  public void reject(RejectionType type, String reason)
//  {
//    // TODO: schedules record and response
//  }
//
//  public void accept(Intent intent, UnpackedObject value)
//  {
//
//  }
//
//  public void accept(Intent intent, UnpackedObject value, boolean respondOnCommit)
//  {
//
//  }

  // TODO: wäre nett, wenn einheitlich wäre, wann Responses gesendet werden, denn dann muss man
  // das bei der Benutzung dieser Klasse wissen

  // TODO: need to differentiate commands and events here

  public void setContext(TypedRecord<V> record)
  {
    this.currentRecord = record;

    batchWriter = streamWriter.newBatch();

    lifecycle.onEnter(currentRecord);
  }

  public void publishEvent(I intent, UnpackedObject value)
  {
    publishEvent(keyGenerator.nextKey(), intent, value);
  }

  public void publishEvent(long key, I intent, UnpackedObject value)
  {
    // TODO: stages record for writing
    batchWriter.addFollowUpEvent(key, intent, value);

    lifecycle.onPublish(currentRecord, key, intent, value);
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
