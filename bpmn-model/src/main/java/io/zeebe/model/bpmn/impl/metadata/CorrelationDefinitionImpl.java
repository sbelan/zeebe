/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.model.bpmn.impl.metadata;

import io.zeebe.model.bpmn.BpmnConstants;
import io.zeebe.model.bpmn.impl.instance.BaseElement;
import io.zeebe.model.bpmn.instance.CorrelationDefinition;
import io.zeebe.msgpack.jsonpath.JsonPathQuery;
import io.zeebe.msgpack.jsonpath.JsonPathQueryCompiler;
import java.beans.Transient;
import javax.xml.bind.annotation.XmlAttribute;

public class CorrelationDefinitionImpl extends BaseElement implements CorrelationDefinition {
  private String messageName;
  private String eventTopic;

  private String eventKeyQuery;
  private JsonPathQuery eventKey;

  @Override
  public String getMessageName() {
    return messageName;
  }

  @XmlAttribute(name = BpmnConstants.ZEEBE_ATTRIBUTE_CORRELATION_MESSAGE_NAME)
  public void setMessageName(String messageName) {
    this.messageName = messageName;
  }

  @Transient
  @Override
  public JsonPathQuery getEventKey() {
    return eventKey;
  }

  public String getEventKeyQuery() {
    return eventKeyQuery;
  }

  @XmlAttribute(name = BpmnConstants.ZEEBE_ATTRIBUTE_CORRELATION_EVENT_KEY)
  public void setEventKeyQuery(String eventKeyQuery) {
    this.eventKeyQuery = eventKeyQuery;
    this.eventKey = new JsonPathQueryCompiler().compile(eventKeyQuery);
  }

  @Override
  public String getEventTopic() {
    return eventTopic;
  }

  @XmlAttribute(name = BpmnConstants.ZEEBE_ATTRIBUTE_CORRELATION_EVENT_TOPIC)
  public void setEventTopic(String eventTopic) {
    this.eventTopic = eventTopic;
  };
}
