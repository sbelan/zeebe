package io.zeebe.broker.workflow.processor.v2.handler;

import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.broker.workflow.processor.v2.BpmnStepContext;
import io.zeebe.broker.workflow.processor.v2.BpmnStepHandler;
import io.zeebe.model.bpmn.instance.FlowNode;
import io.zeebe.model.bpmn.instance.SequenceFlow;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class TakeSequenceFlowHandler implements BpmnStepHandler<FlowNode> {

  @Override
  public void handle(BpmnStepContext<FlowNode> context) {
    final FlowNode flowNode = context.getElement();

    // the activity has exactly one outgoing sequence flow
    final SequenceFlow sequenceFlow = flowNode.getOutgoingSequenceFlows().get(0);

    final WorkflowInstanceRecord value = context.getCurrentValue();
    value.setActivityId(sequenceFlow.getIdAsBuffer());

    context.getRecordWriter().publish(WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN, value);
  }

}
