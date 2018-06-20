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
package io.zeebe.model.bpmn.impl.instance;

import java.util.HashMap;
import java.util.Map;
import javax.xml.bind.annotation.XmlAttribute;
import org.agrona.DirectBuffer;
import io.zeebe.model.bpmn.BpmnConstants;
import io.zeebe.model.bpmn.instance.FlowElement;
import io.zeebe.model.bpmn.instance.Workflow;

public class ProcessImpl extends FlowElementContainer implements Workflow {
  private boolean isExecutable = true;

  private final Map<DirectBuffer, FlowElement> flowElementMap = new HashMap<>();

  @XmlAttribute(name = BpmnConstants.BPMN_ATTRIBUTE_IS_EXECUTABLE)
  public void setExecutable(boolean isExecutable) {
    this.isExecutable = isExecutable;
  }

  @Override
  public boolean isExecutable() {
    return isExecutable;
  }


  @Override
  public DirectBuffer getBpmnProcessId() {
    return getIdAsBuffer();
  }

  @SuppressWarnings("unchecked")
  public <T extends FlowElement> T findFlowElementById(DirectBuffer id) {
    return (T) flowElementMap.get(id);
  }

  public Map<DirectBuffer, FlowElement> getFlowElementMap() {
    return flowElementMap;
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("Workflow [isExecutable=");
    builder.append(isExecutable);
    builder.append(", bpmnProcessId=");
    builder.append(getId());
    builder.append(", name=");
    builder.append(getName());
    builder.append("]");
    return builder.toString();
  }
}
