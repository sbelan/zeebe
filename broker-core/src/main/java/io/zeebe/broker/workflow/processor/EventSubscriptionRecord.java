package io.zeebe.broker.workflow.processor;

import org.agrona.DirectBuffer;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.msgpack.property.LongProperty;
import io.zeebe.msgpack.property.StringProperty;

public class EventSubscriptionRecord extends UnpackedObject {

  private LongProperty workflowInstanceKey = new LongProperty("workflowInstanceKey");
  private LongProperty eventScopeKey = new LongProperty("eventScopeKey");
  private StringProperty eventHandler = new StringProperty("eventHandler");

  public EventSubscriptionRecord() {
    declareProperty(eventScopeKey)
    .declareProperty(workflowInstanceKey)
    .declareProperty(eventHandler);
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

  public void setEventHandler(DirectBuffer eventHandler) {
    this.eventHandler.setValue(eventHandler);
  }

  public DirectBuffer getEventHandler() {
    return eventHandler.getValue();
  }
}
