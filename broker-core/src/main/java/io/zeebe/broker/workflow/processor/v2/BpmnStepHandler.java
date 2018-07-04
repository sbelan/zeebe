package io.zeebe.broker.workflow.processor.v2;

import io.zeebe.model.bpmn.instance.FlowElement;
import io.zeebe.model.bpmn.instance.WorkflowDefinition;

public interface BpmnStepHandler<T extends FlowElement> {

  void handle(BpmnStepContext<T> context);

}
