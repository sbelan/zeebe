package io.zeebe.model.bpmn.impl.transformation;

import java.util.EnumMap;
import java.util.List;
import io.zeebe.model.bpmn.impl.instance.ExclusiveGatewayImpl;
import io.zeebe.model.bpmn.instance.BpmnStep;
import io.zeebe.model.bpmn.instance.SequenceFlow;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class ExclusiveGatewayLifecycleFactory extends FlowNodeLifecycleFactory<ExclusiveGatewayImpl> {

  @Override
  public EnumMap<WorkflowInstanceIntent, BpmnStep> buildLifecycle(ExclusiveGatewayImpl element) {
    final EnumMap<WorkflowInstanceIntent, BpmnStep> lifecycle = super.buildLifecycle(element);

    final List<SequenceFlow> outgoingSequenceFlows = element.getOutgoingSequenceFlows();
    if (outgoingSequenceFlows.size() > 0 && outgoingSequenceFlows.get(0).hasCondition())
    {
      // Override default outgoing behavior
      lifecycle.put(getCompletingIntent(), BpmnStep.EXCLUSIVE_SPLIT);
    }

    return lifecycle;
  }

  @Override
  protected WorkflowInstanceIntent getCompletingIntent() {
    return WorkflowInstanceIntent.GATEWAY_ACTIVATED;
  }

}
