package io.zeebe.model.bpmn.builder;

import io.zeebe.model.bpmn.impl.instance.ExtensionElementsImpl;
import io.zeebe.model.bpmn.impl.instance.MergeInstruction;
import io.zeebe.model.bpmn.impl.instance.ParallelGatewayImpl;

public class BpmnParallelGatewayBuilder {

  private ParallelGatewayImpl gateway;
  private BpmnBuilder builder;

  public BpmnParallelGatewayBuilder(BpmnBuilder builder, ParallelGatewayImpl gateway) {
    this.builder = builder;
    this.gateway = gateway;

    gateway.setExtensionElements(new ExtensionElementsImpl());
  }

  public BpmnParallelGatewayBuilder mergeInstructionPut(String flowId, String sourceJsonPath, String targetJsonPath) {
    final MergeInstruction instruction = new MergeInstruction();
    instruction.setType(MergeInstruction.Type.PUT);
    instruction.setElement(flowId);
    instruction.setSource(sourceJsonPath);
    instruction.setTarget(targetJsonPath);
    gateway.getExtensionElements().getMergeInstructions().add(instruction);
    return this;
  }

  public BpmnParallelGatewayBuilder mergeInstructionRemove(String targetJsonPath) {
    final MergeInstruction instruction = new MergeInstruction();
    instruction.setType(MergeInstruction.Type.REMOVE);
    instruction.setTarget(targetJsonPath);
    gateway.getExtensionElements().getMergeInstructions().add(instruction);
    return this;
  }

  public BpmnBuilder done() {
    return builder;
  }
}
