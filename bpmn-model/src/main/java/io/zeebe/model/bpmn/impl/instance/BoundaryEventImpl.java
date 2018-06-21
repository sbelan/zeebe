package io.zeebe.model.bpmn.impl.instance;

import javax.xml.bind.annotation.XmlAttribute;
import org.agrona.DirectBuffer;
import io.zeebe.util.buffer.BufferUtil;

public class BoundaryEventImpl extends FlowNodeImpl {

  private DirectBuffer attachedToRef;

  @XmlAttribute(name = "attachedToRef", required = true)
  public void setAttachedToRef(String attachedToRef) {
    this.attachedToRef = BufferUtil.wrapString(attachedToRef);
  }

  public DirectBuffer getAttachedToRef() {
    return attachedToRef;
  }
}
