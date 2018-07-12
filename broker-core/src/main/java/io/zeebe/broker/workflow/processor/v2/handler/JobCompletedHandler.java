package io.zeebe.broker.workflow.processor.v2.handler;

import io.zeebe.broker.job.data.JobHeaders;
import io.zeebe.broker.job.data.JobRecord;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.broker.workflow.processor.v2.ActivityInstance;
import io.zeebe.broker.workflow.processor.v2.RecordHandler;
import io.zeebe.broker.workflow.processor.v2.RecordWriter;
import io.zeebe.broker.workflow.processor.v2.WorkflowInstance;
import io.zeebe.broker.workflow.processor.v2.WorkflowInstances;
import io.zeebe.logstreams.processor.EventLifecycleContext;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class JobCompletedHandler implements RecordHandler<JobRecord> {
  private final WorkflowInstanceRecord workflowInstanceEvent = new WorkflowInstanceRecord();
  private final WorkflowInstances workflowInstances;

  public JobCompletedHandler(WorkflowInstances workflowInstances)
  {
    this.workflowInstances = workflowInstances;
  }

  private long activityInstanceKey;

  @Override
  public void handle(RecordWriter recordWriter, TypedRecord<JobRecord> record, EventLifecycleContext ctx) {
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
        if (activityInstance != null)
        {
          workflowInstanceEvent
            .setBpmnProcessId(jobHeaders.getBpmnProcessId())
            .setVersion(jobHeaders.getWorkflowDefinitionVersion())
            .setWorkflowKey(jobHeaders.getWorkflowKey())
            .setWorkflowInstanceKey(jobHeaders.getWorkflowInstanceKey())
            .setActivityId(jobHeaders.getActivityId())
            .setPayload(jobEvent.getPayload());

          recordWriter.publishEvent(activityInstanceKey,
              WorkflowInstanceIntent.ACTIVITY_COMPLETING,
              workflowInstanceEvent);

          activityInstance.removeJob(record);
        }
      }
    }
  }
}
