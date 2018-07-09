package io.zeebe.broker.workflow.processor.v2.handler;

import io.zeebe.broker.job.data.JobRecord;
import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.broker.workflow.processor.v2.BpmnStepContext;
import io.zeebe.broker.workflow.processor.v2.BpmnStepHandler;
import io.zeebe.model.bpmn.instance.ServiceTask;
import io.zeebe.model.bpmn.instance.TaskDefinition;
import io.zeebe.protocol.intent.JobIntent;

public class CreateJobHandler implements BpmnStepHandler<ServiceTask> {

  private final JobRecord jobCommand = new JobRecord();

  @Override
  public void handle(BpmnStepContext<ServiceTask> context) {
    final ServiceTask serviceTask = context.getElement();
    final TaskDefinition taskDefinition = serviceTask.getTaskDefinition();

    final WorkflowInstanceRecord value = context.getCurrentValue();

    jobCommand.reset();

    jobCommand
        .setType(taskDefinition.getTypeAsBuffer())
        .setRetries(taskDefinition.getRetries())
        .setPayload(value.getPayload())
        .headers()
        .setBpmnProcessId(value.getBpmnProcessId())
        .setWorkflowDefinitionVersion(value.getVersion())
        .setWorkflowKey(value.getWorkflowKey())
        .setWorkflowInstanceKey(value.getWorkflowInstanceKey())
        .setActivityId(serviceTask.getIdAsBuffer())
        .setActivityInstanceKey(context.getCurrentRecord().getKey());

    final io.zeebe.model.bpmn.instance.TaskHeaders customHeaders = serviceTask.getTaskHeaders();

    if (!customHeaders.isEmpty()) {
      jobCommand.setCustomHeaders(customHeaders.asMsgpackEncoded());
    }

    context.getRecordWriter().publishCommand(JobIntent.CREATE, jobCommand);
  }

}
