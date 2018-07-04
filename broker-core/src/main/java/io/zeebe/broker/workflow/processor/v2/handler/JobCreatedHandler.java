package io.zeebe.broker.workflow.processor.v2.handler;

import io.zeebe.broker.job.data.JobHeaders;
import io.zeebe.broker.job.data.JobRecord;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.workflow.processor.v2.ActivityInstance;
import io.zeebe.broker.workflow.processor.v2.WorkflowInstance;
import io.zeebe.broker.workflow.processor.v2.WorkflowInstances;

public class JobCreatedHandler implements TypedRecordProcessor<JobRecord> {

  private WorkflowInstances workflowInstances;

  @Override
  public void processRecord(TypedRecord<JobRecord> record) {
    final JobHeaders jobHeaders = record.getValue().headers();

    final WorkflowInstance workflowInstance = workflowInstances
        .getWorkflowInstance(jobHeaders.getWorkflowInstanceKey());

    if (workflowInstance != null)
    {
      final ActivityInstance activityInstance = workflowInstance
          .getActivityInstance(jobHeaders.getActivityInstanceKey());
      if (activityInstance != null)
      {
        activityInstance.addJob(record);
      }
    }
  }
}
