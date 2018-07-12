package io.zeebe.model.bpmn.impl.transformation;

import java.util.EnumMap;
import io.zeebe.model.bpmn.impl.instance.FlowElementImpl;
import io.zeebe.model.bpmn.instance.BpmnStep;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public interface ElementLifecycleFactory<T extends FlowElementImpl> {

  EnumMap<WorkflowInstanceIntent, BpmnStep> buildLifecycle(T element);
}
