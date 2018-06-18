package io.zeebe.model.bpmn.impl.metadata;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

import io.zeebe.model.bpmn.BpmnConstants;
import io.zeebe.model.bpmn.impl.instance.BaseElement;
import io.zeebe.model.bpmn.instance.CorrelationDefinition;
import io.zeebe.model.bpmn.instance.CorrelationEventKeyDefinition;

public class CorrelationDefinitionImpl extends BaseElement implements CorrelationDefinition
{
    private List<CorrelationEventKeyDefinition> eventKeyDefinitions = new ArrayList<>();

    private String messageName;

    @Override
    public String getMessageName()
    {
        return messageName;
    }

    @Override
    public List<CorrelationEventKeyDefinition> getEventKeyDefinitions()
    {
        return eventKeyDefinitions;
    }

    @XmlElement(name = BpmnConstants.ZEEBE_ELEMENT_CORRELATION_EVENT_KEY_DEFINITION, namespace = BpmnConstants.ZEEBE_NS, type = CorrelationEventKeyDefinitionImpl.class)
    public void setEventKeyDefinitions(List<CorrelationEventKeyDefinition> eventKeyDefinitions)
    {
        this.eventKeyDefinitions = eventKeyDefinitions;
    }

    @XmlAttribute(name = BpmnConstants.ZEEBE_ATTRIBUTE_CORRELATION_MESSAGE_NAME)
    public void setMessageName(String messageName)
    {
        this.messageName = messageName;
    };

}
