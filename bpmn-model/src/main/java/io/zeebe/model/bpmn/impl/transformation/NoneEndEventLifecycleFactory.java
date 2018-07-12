package io.zeebe.model.bpmn.impl.transformation;

import io.zeebe.model.bpmn.impl.instance.FlowNodeImpl;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class NoneEndEventLifecycleFactory extends FlowNodeLifecycleFactory<FlowNodeImpl> {

  @Override
  protected WorkflowInstanceIntent getCompletingIntent() {
    return WorkflowInstanceIntent.END_EVENT_OCCURRED;
  }

}
