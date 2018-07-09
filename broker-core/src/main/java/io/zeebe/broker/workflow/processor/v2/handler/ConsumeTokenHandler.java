package io.zeebe.broker.workflow.processor.v2.handler;

import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.broker.workflow.processor.v2.ActivityInstance;
import io.zeebe.broker.workflow.processor.v2.BpmnStepContext;
import io.zeebe.broker.workflow.processor.v2.BpmnStepHandler;
import io.zeebe.broker.workflow.processor.v2.WorkflowInstance;
import io.zeebe.model.bpmn.instance.FlowElement;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class ConsumeTokenHandler implements BpmnStepHandler<FlowElement> {

  @Override
  public void handle(BpmnStepContext<FlowElement> context) {

    final WorkflowInstanceRecord value = context.getCurrentValue();
    final WorkflowInstance workflowInstance = context.getWorkflowInstance();

    final ActivityInstance rootScope = workflowInstance.getRootScope();
    rootScope.consumeTokens(1);

    if (rootScope.getActiveTokens() == 0)
    {
      value.setActivityId("");
      context.getRecordWriter().publishEvent(value.getWorkflowInstanceKey(), WorkflowInstanceIntent.COMPLETED, value);
    }
  }

}
