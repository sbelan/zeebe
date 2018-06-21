package io.zeebe.broker.workflow.processor;

import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.msgpack.property.LongProperty;

public class EventSubscriptionRecord extends UnpackedObject {

  private LongProperty workflowInstanceKey = new LongProperty("workflowInstanceKey");
  private LongProperty eventScopeKey = new LongProperty("eventScopeKey");

  public EventSubscriptionRecord() {
    declareProperty(eventScopeKey)
    .declareProperty(workflowInstanceKey);
  }

  public EventSubscriptionRecord setEventScopeKey(long eventScopeKey) {
    this.eventScopeKey.setValue(eventScopeKey);
    return this;
  }

  public long getEventScopeKey() {
    return eventScopeKey.getValue();
  }

  public void setWorkflowInstanceKey(long workflowInstanceKey) {
    this.workflowInstanceKey.setValue(workflowInstanceKey);
  }

  public long getWorkflowInstanceKey() {
    return workflowInstanceKey.getValue();
  }
}
