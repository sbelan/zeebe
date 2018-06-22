package io.zeebe.model.bpmn.impl.instance;

import javax.xml.bind.annotation.XmlAttribute;

public class SubProcessImpl extends FlowElementContainer {

  private boolean triggeredByEvent = false;

  @XmlAttribute(name = "triggeredByEvent")
  public void setTriggeredByEvent(boolean triggeredByEvent) {
    this.triggeredByEvent = triggeredByEvent;
  }

  public boolean isTriggeredByEvent() {
    return triggeredByEvent;
  }
}
