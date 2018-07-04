package io.zeebe.broker.workflow.processor.v2.handler;

import io.zeebe.broker.job.data.JobHeaders;
import io.zeebe.broker.job.data.JobRecord;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.broker.workflow.processor.v2.ActivityInstance;
import io.zeebe.broker.workflow.processor.v2.WorkflowInstance;
import io.zeebe.broker.workflow.processor.v2.WorkflowInstances;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class JobCompletedHandler implements TypedRecordProcessor<JobRecord> {
  private final WorkflowInstanceRecord workflowInstanceEvent = new WorkflowInstanceRecord();
  private final WorkflowInstances workflowInstances;

  public JobCompletedHandler(WorkflowInstances workflowInstances)
  {
    this.workflowInstances = workflowInstances;
  }

  private boolean activityCompleted;
  private long activityInstanceKey;

  @Override
  public void processRecord(TypedRecord<JobRecord> record) {
    activityCompleted = false;

    final JobRecord jobEvent = record.getValue();
    final JobHeaders jobHeaders = jobEvent.headers();
    activityInstanceKey = jobHeaders.getActivityInstanceKey();

    final long workflowInstanceKey = jobHeaders.getWorkflowInstanceKey();
    if (workflowInstanceKey > 0)
    {
      final WorkflowInstance workflowInstance = workflowInstances.getWorkflowInstance(workflowInstanceKey);
      if (workflowInstance != null)
      {
        final ActivityInstance activityInstance = workflowInstance.getActivityInstance(jobHeaders.getActivityInstanceKey());
        if (activityInstance != null) // && assert it is active
        {
          activityCompleted = true;
          activityInstance.removeJob(record);
        }
      }
    }
  }

  @Override
  public long writeRecord(TypedRecord<JobRecord> record, TypedStreamWriter writer) {
    return activityCompleted
        ? writer.writeFollowUpEvent(
            activityInstanceKey,
            WorkflowInstanceIntent.ACTIVITY_COMPLETING,
            workflowInstanceEvent)
        : 0L;
  }
}
