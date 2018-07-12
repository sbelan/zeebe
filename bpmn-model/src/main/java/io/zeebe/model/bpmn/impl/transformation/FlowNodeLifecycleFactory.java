package io.zeebe.model.bpmn.impl.transformation;

import java.util.EnumMap;
import java.util.List;
import io.zeebe.model.bpmn.impl.instance.FlowNodeImpl;
import io.zeebe.model.bpmn.instance.BpmnStep;
import io.zeebe.model.bpmn.instance.SequenceFlow;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public abstract class FlowNodeLifecycleFactory<T extends FlowNodeImpl> implements ElementLifecycleFactory<T> {

  @Override
  public EnumMap<WorkflowInstanceIntent, BpmnStep> buildLifecycle(T element) {
    final EnumMap<WorkflowInstanceIntent, BpmnStep> lifecycle = new EnumMap<>(WorkflowInstanceIntent.class);

    final WorkflowInstanceIntent completingEvent = getCompletingIntent();

    final List<SequenceFlow> outgoingSequenceFlows = element.getOutgoingSequenceFlows();
    if (outgoingSequenceFlows.isEmpty()) {
      lifecycle.put(completingEvent, BpmnStep.SCOPE_MERGE);
    } else if (outgoingSequenceFlows.size() == 1
        && !outgoingSequenceFlows.get(0).hasCondition()) {
      lifecycle.put(completingEvent, BpmnStep.TAKE_SEQUENCE_FLOW);
    }

    return lifecycle;
  }

  protected abstract WorkflowInstanceIntent getCompletingIntent();
}
