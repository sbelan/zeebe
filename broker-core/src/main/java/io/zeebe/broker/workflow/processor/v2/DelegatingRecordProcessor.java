package io.zeebe.broker.workflow.processor.v2;

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.logstreams.processor.TypedStreamProcessor;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriterImpl;
import io.zeebe.logstreams.processor.EventLifecycleContext;
import io.zeebe.msgpack.UnpackedObject;

public class DelegatingRecordProcessor<T extends UnpackedObject> implements TypedRecordProcessor<T> {

  private final RecordHandler<T> handler;
  private final RecordWriter writer;

  // TODO: when consolidating stream processor, this must go away
  private int producerId;

  public DelegatingRecordProcessor(TypedStreamEnvironment env, RecordHandler<T> handler) {
    this.writer = new RecordWriter((TypedStreamWriterImpl) env.getStreamWriter(), new ResponseWriter(env.getOutput(), env.getStream().getPartitionId()));
    this.handler = handler;
  }

  @Override
  public void processRecord(TypedRecord<T> record, EventLifecycleContext ctx) {
    writer.reset(producerId, record.getPosition());
    handler.handle(writer, record, ctx);
  }

  @Override
  public long writeRecord(TypedRecord<T> record, TypedStreamWriter writer) {
    return this.writer.flushEvents();
  }

  @Override
  public boolean executeSideEffects(TypedRecord<T> record, TypedResponseWriter responseWriter) {
    return this.writer.flushSideEffects();
  }

  @Override
  public void onOpen(TypedStreamProcessor streamProcessor) {
    handler.onOpen(streamProcessor);
    producerId = streamProcessor.getStreamProcessorContext().getId();
  }

  @Override
  public void onClose() {
    handler.onClose();
  }

  @Override
  public void onRecovered(TypedStreamProcessor streamProcessor) {
    handler.onRecovered(streamProcessor);
  }


}
