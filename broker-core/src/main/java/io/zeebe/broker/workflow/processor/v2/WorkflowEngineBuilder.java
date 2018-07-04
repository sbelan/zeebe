package io.zeebe.broker.workflow.processor.v2;

import java.util.EnumMap;

public class WorkflowEngineBuilder {

  private EnumMap<BpmnStep, BpmnStepHandler<?>> handlers = new EnumMap<>(BpmnStep.class);

  public WorkflowEngineBuilder onBpmnStep(BpmnStep step, BpmnStepHandler<?> stepHandler) {
    this.handlers.put(step, stepHandler);
    return this;
  }

}
