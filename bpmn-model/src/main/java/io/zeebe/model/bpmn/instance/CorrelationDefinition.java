package io.zeebe.model.bpmn.instance;

import java.util.List;

public interface CorrelationDefinition
{
    String getMessageName();

    List<CorrelationEventKeyDefinition> getEventKeyDefinitions();
}
