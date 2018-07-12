package io.zeebe.model.bpmn.impl.transformation;

import java.util.EnumMap;
import io.zeebe.model.bpmn.impl.instance.EndEventImpl;
import io.zeebe.model.bpmn.impl.instance.ExclusiveGatewayImpl;
import io.zeebe.model.bpmn.impl.instance.SequenceFlowImpl;
import io.zeebe.model.bpmn.impl.instance.ServiceTaskImpl;
import io.zeebe.model.bpmn.instance.BpmnStep;
import io.zeebe.model.bpmn.instance.EndEvent;
import io.zeebe.model.bpmn.instance.ExclusiveGateway;
import io.zeebe.model.bpmn.instance.FlowNode;
import io.zeebe.model.bpmn.instance.ServiceTask;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import scala.collection.immutable.NumericRange.Exclusive;

public class SequenceFlowLifecycleFactory implements ElementLifecycleFactory<SequenceFlowImpl> {

  private static final EnumMap<WorkflowInstanceIntent, BpmnStep> START_ACTIVITY = new EnumMap<>(WorkflowInstanceIntent.class);
  private static final EnumMap<WorkflowInstanceIntent, BpmnStep> TRIGGER_NONE_EVENT = new EnumMap<>(WorkflowInstanceIntent.class);
  private static final EnumMap<WorkflowInstanceIntent, BpmnStep> ACTIVATE_GATEWAY = new EnumMap<>(WorkflowInstanceIntent.class);

  static
  {
    START_ACTIVITY.put(WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN, BpmnStep.START_ACTIVITY);

    TRIGGER_NONE_EVENT.put(WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN, BpmnStep.TRIGGER_NONE_EVENT);

    ACTIVATE_GATEWAY.put(WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN, BpmnStep.ACTIVATE_GATEWAY);
  }

  @Override
  public EnumMap<WorkflowInstanceIntent, BpmnStep> buildLifecycle(SequenceFlowImpl element) {
    final FlowNode targetNode = element.getTargetNode();

    if (targetNode instanceof ServiceTaskImpl)
    {
      return START_ACTIVITY;
    }
    else if (targetNode instanceof EndEventImpl)
    {
      return TRIGGER_NONE_EVENT;
    }
    else if (targetNode instanceof ExclusiveGatewayImpl)
    {
      return ACTIVATE_GATEWAY;
    }
    else
    {
      throw new RuntimeException("unsupported element " + element);
    }
  }

}
