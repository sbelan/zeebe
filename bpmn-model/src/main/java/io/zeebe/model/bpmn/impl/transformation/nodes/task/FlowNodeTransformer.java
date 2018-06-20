package io.zeebe.model.bpmn.impl.transformation.nodes.task;

import java.util.List;
import io.zeebe.model.bpmn.impl.error.ErrorCollector;
import io.zeebe.model.bpmn.impl.instance.ExtensionElementsImpl;
import io.zeebe.model.bpmn.impl.instance.FlowNodeImpl;
import io.zeebe.model.bpmn.impl.metadata.InputOutputMappingImpl;

public class FlowNodeTransformer {

  private final InputOutputMappingTransformer inputOutputMappingTransformer =
      new InputOutputMappingTransformer();


  public void transform(ErrorCollector errorCollector, List<FlowNodeImpl> serviceTasks) {
    for (int s = 0; s < serviceTasks.size(); s++) {
      final FlowNodeImpl serviceTaskImpl = serviceTasks.get(s);

      transformInputOutputMappings(errorCollector, serviceTaskImpl, serviceTaskImpl.getExtensionElements());
    }
  }

  public void transformInputOutputMappings(
      ErrorCollector errorCollector,
      FlowNodeImpl flowNodeImpl,
      ExtensionElementsImpl extensionElements) {
    if (extensionElements == null)
    {
      extensionElements = new ExtensionElementsImpl();
      flowNodeImpl.setExtensionElements(extensionElements);
    }

    InputOutputMappingImpl inputOutputMapping = flowNodeImpl.getInputOutputMapping();
    if (inputOutputMapping == null) {
      inputOutputMapping = new InputOutputMappingImpl();
      extensionElements.setInputOutputMapping(inputOutputMapping);
    }
    inputOutputMappingTransformer.transform(errorCollector, inputOutputMapping);
  }
}
