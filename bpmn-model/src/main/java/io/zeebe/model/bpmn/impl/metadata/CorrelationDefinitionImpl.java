package io.zeebe.model.bpmn.impl.metadata;

import javax.xml.bind.annotation.XmlAttribute;

import io.zeebe.model.bpmn.BpmnConstants;
import io.zeebe.model.bpmn.impl.instance.BaseElement;
import io.zeebe.model.bpmn.instance.CorrelationDefinition;

public class CorrelationDefinitionImpl extends BaseElement implements CorrelationDefinition
{
    private String messageName;
    private String eventKey;
    private String eventTopic;

    @Override
    public String getMessageName()
    {
        return messageName;
    }

    @XmlAttribute(name = BpmnConstants.ZEEBE_ATTRIBUTE_CORRELATION_MESSAGE_NAME)
    public void setMessageName(String messageName)
    {
        this.messageName = messageName;
    }

    @Override
    public String getEventKey()
    {
        return eventKey;
    }

    @XmlAttribute(name = BpmnConstants.ZEEBE_ATTRIBUTE_CORRELATION_EVENT_KEY)
    public void setEventKey(String eventKey)
    {
        this.eventKey = eventKey;
    }

    @Override
    public String getEventTopic()
    {
        return eventTopic;
    }

    @XmlAttribute(name = BpmnConstants.ZEEBE_ATTRIBUTE_CORRELATION_EVENT_TOPIC)
    public void setEventTopic(String eventTopic)
    {
        this.eventTopic = eventTopic;
    };

}
