package io.zeebe.broker.workflow.processor.v2.handler;

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.broker.workflow.processor.v2.RecordHandler;
import io.zeebe.broker.workflow.processor.v2.RecordWriter;
import io.zeebe.logstreams.processor.EventLifecycleContext;
import io.zeebe.protocol.impl.RecordMetadata;

public class WorkflowInstanceRejectedProcessor implements RecordHandler<WorkflowInstanceRecord> {

  @Override
  public void handle(RecordWriter recordWriter, TypedRecord<WorkflowInstanceRecord> record,
      EventLifecycleContext ctx) {
    final RecordMetadata metadata = record.getMetadata();
    // this is inefficient, but we are going to remove this record handler in the future anyway
    // => https://github.com/zeebe-io/zeebe/issues/1040
    final String rejectionReason = metadata.getRejectionReason().getStringWithoutLengthAscii(0, metadata.getRejectionReason().capacity());
    recordWriter.sendReject(record, metadata.getRejectionType(), rejectionReason);

  }

}
