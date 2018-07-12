package io.zeebe.model.bpmn.impl.transformation;

import io.zeebe.model.bpmn.impl.instance.FlowNodeImpl;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class NoneStartEventLifecycleFactory extends FlowNodeLifecycleFactory<FlowNodeImpl> {

  @Override
  protected WorkflowInstanceIntent getCompletingIntent() {
    return WorkflowInstanceIntent.START_EVENT_OCCURRED;
  }

}
