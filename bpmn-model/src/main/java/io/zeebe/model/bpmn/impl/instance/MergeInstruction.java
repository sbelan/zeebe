package io.zeebe.model.bpmn.impl.instance;

import javax.xml.bind.annotation.XmlAttribute;
import org.agrona.DirectBuffer;
import io.zeebe.util.buffer.BufferUtil;

public class MergeInstruction {

  private Type type;
  private DirectBuffer source;
  private DirectBuffer target;
  private DirectBuffer element;

  @XmlAttribute(name = "type")
  public void setType(Type type) {
    this.type = type;
  }

  public Type getType() {
    return type;
  }

  @XmlAttribute(name = "source")
  public void setSource(String source) {
    this.source = BufferUtil.wrapString(source);
  }

  public DirectBuffer getSource() {
    return source;
  }

  @XmlAttribute(name = "target")
  public void setTarget(String target) {
    this.target = BufferUtil.wrapString(target);
  }

  public DirectBuffer getTarget() {
    return target;
  }

  @XmlAttribute(name = "type")
  public void setElement(String element) {
    this.element = BufferUtil.wrapString(element);
  }

  public DirectBuffer getElement() {
    return element;
  }

  public enum Type
  {
    PUT,
    REMOVE
  }


}
