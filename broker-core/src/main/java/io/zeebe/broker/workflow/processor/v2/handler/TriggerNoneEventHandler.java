package io.zeebe.broker.workflow.processor.v2.handler;

import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.broker.workflow.processor.v2.BpmnStepContext;
import io.zeebe.broker.workflow.processor.v2.BpmnStepHandler;
import io.zeebe.model.bpmn.instance.FlowNode;
import io.zeebe.model.bpmn.instance.SequenceFlow;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class TriggerNoneEventHandler implements BpmnStepHandler<SequenceFlow> {

  @Override
  public void handle(BpmnStepContext<SequenceFlow> context) {

    final FlowNode targetNode = context.getElement().getTargetNode();

    final WorkflowInstanceRecord value = context.getCurrentValue();
    value.setActivityId(targetNode.getIdAsBuffer());

    context.getRecordWriter().publish(WorkflowInstanceIntent.END_EVENT_OCCURRED, value);
  }

}
