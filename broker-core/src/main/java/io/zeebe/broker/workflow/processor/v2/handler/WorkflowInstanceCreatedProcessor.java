package io.zeebe.broker.workflow.processor.v2.handler;

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.broker.workflow.processor.v2.RecordHandler;
import io.zeebe.broker.workflow.processor.v2.RecordWriter;
import io.zeebe.logstreams.processor.EventLifecycleContext;

public class WorkflowInstanceCreatedProcessor implements RecordHandler<WorkflowInstanceRecord> {

  @Override
  public void handle(RecordWriter recordWriter, TypedRecord<WorkflowInstanceRecord> record,
      EventLifecycleContext ctx) {

    recordWriter.sendAccept(record, record.getMetadata().getIntent());
  }

}
