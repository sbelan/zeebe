package io.zeebe.broker.workflow.processor.v2;

import io.zeebe.broker.job.data.JobRecord;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.workflow.processor.v2.handler.JobCompletedHandler;
import io.zeebe.logstreams.processor.EventLifecycleContext;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.util.sched.ActorControl;

public class JobLifecycle implements Lifecycle<JobIntent, JobRecord>{

  private WorkflowInstances wfInstances;

  public JobLifecycle(WorkflowInstances wfInstances)
  {
    this.wfInstances = wfInstances;
  }

  @Override
  public void process(RecordWriter recordWriter, TypedRecord<JobRecord> record,
      EventLifecycleContext ct) {
    if (record.getMetadata().getIntent() == JobIntent.COMPLETED)
    {
      new JobCompletedHandler(wfInstances).handle(recordWriter, record);
    }

  }

  @Override
  public void onPublish(long key, JobIntent intent, JobRecord valuex) {
  }

  @Override
  public void onOpen(ActorControl streamProcessorActor) {
  }

}
