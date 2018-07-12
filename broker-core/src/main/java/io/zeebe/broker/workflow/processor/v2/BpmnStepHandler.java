package io.zeebe.broker.workflow.processor.v2;

import io.zeebe.model.bpmn.instance.FlowElement;
import io.zeebe.model.bpmn.instance.WorkflowDefinition;

public interface BpmnStepHandler<T extends FlowElement> {

  void handle(BpmnStepContext<T> context);

  default boolean applies(BpmnStepContext<T> context)
  {
    // TODO: This must be implemented to avoid continuing execution when something is terminated concurrently
    return true;
  };

}
