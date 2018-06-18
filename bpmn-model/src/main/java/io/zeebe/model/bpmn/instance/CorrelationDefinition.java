package io.zeebe.model.bpmn.instance;

public interface CorrelationDefinition
{
    String getMessageName();

    String getEventKey();
}
