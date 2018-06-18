package io.zeebe.model.bpmn.impl.metadata;

import javax.xml.bind.annotation.XmlAttribute;

import io.zeebe.model.bpmn.BpmnConstants;
import io.zeebe.model.bpmn.impl.instance.BaseElement;
import io.zeebe.model.bpmn.instance.CorrelationEventKeyDefinition;

public class CorrelationEventKeyDefinitionImpl extends BaseElement implements CorrelationEventKeyDefinition
{
    private String name;
    private String value;

    @Override
    public String getName()
    {
        return name;
    }

    @XmlAttribute(name = BpmnConstants.ZEEBE_ATTRIBUTE_CORRELATION_EVENT_KEY_NAME)
    public void setName(String name)
    {
        this.name = name;
    }

    @Override
    public String getValue()
    {
        return value;
    }

    @XmlAttribute(name = BpmnConstants.ZEEBE_ATTRIBUTE_CORRELATION_EVENT_KEY_VALUE)
    public void setValue(String value)
    {
        this.value = value;
    }

}
