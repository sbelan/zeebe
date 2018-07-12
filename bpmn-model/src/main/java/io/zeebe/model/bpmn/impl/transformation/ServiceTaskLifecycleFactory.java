package io.zeebe.model.bpmn.impl.transformation;

import java.util.EnumMap;
import io.zeebe.model.bpmn.impl.instance.ServiceTaskImpl;
import io.zeebe.model.bpmn.instance.BpmnStep;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class ServiceTaskLifecycleFactory extends FlowNodeLifecycleFactory<ServiceTaskImpl> {

  @Override
  public EnumMap<WorkflowInstanceIntent, BpmnStep> buildLifecycle(ServiceTaskImpl element) {
    final EnumMap<WorkflowInstanceIntent, BpmnStep> lifecycle = super.buildLifecycle(element);

    lifecycle.put(WorkflowInstanceIntent.ACTIVITY_READY, BpmnStep.ACTIVATE_ACTIVITY);
    lifecycle.put(WorkflowInstanceIntent.ACTIVITY_ACTIVATED, BpmnStep.CREATE_JOB);
    lifecycle.put(WorkflowInstanceIntent.ACTIVITY_COMPLETING, BpmnStep.COMPLETE_ACTIVITY);

    return lifecycle;
  }

  @Override
  protected WorkflowInstanceIntent getCompletingIntent() {
    return WorkflowInstanceIntent.ACTIVITY_COMPLETED;
  }

}
