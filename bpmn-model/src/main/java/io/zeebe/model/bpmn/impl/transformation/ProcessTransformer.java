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
package io.zeebe.model.bpmn.impl.transformation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.agrona.DirectBuffer;
import io.zeebe.model.bpmn.BpmnAspect;
import io.zeebe.model.bpmn.impl.error.ErrorCollector;
import io.zeebe.model.bpmn.impl.instance.BoundaryEventImpl;
import io.zeebe.model.bpmn.impl.instance.ExclusiveGatewayImpl;
import io.zeebe.model.bpmn.impl.instance.FlowElementContainer;
import io.zeebe.model.bpmn.impl.instance.FlowElementImpl;
import io.zeebe.model.bpmn.impl.instance.FlowNodeImpl;
import io.zeebe.model.bpmn.impl.instance.ParallelGatewayImpl;
import io.zeebe.model.bpmn.impl.instance.ProcessImpl;
import io.zeebe.model.bpmn.impl.instance.SequenceFlowImpl;
import io.zeebe.model.bpmn.impl.instance.ServiceTaskImpl;
import io.zeebe.model.bpmn.impl.instance.StartEventImpl;
import io.zeebe.model.bpmn.impl.instance.SubProcessImpl;
import io.zeebe.model.bpmn.impl.transformation.nodes.ExclusiveGatewayTransformer;
import io.zeebe.model.bpmn.impl.transformation.nodes.SequenceFlowTransformer;
import io.zeebe.model.bpmn.impl.transformation.nodes.task.FlowNodeTransformer;
import io.zeebe.model.bpmn.impl.transformation.nodes.task.ServiceTaskTransformer;
import io.zeebe.model.bpmn.instance.EndEvent;
import io.zeebe.model.bpmn.instance.ExclusiveGateway;
import io.zeebe.model.bpmn.instance.FlowNode;
import io.zeebe.model.bpmn.instance.SequenceFlow;
import io.zeebe.model.bpmn.instance.ServiceTask;

public class ProcessTransformer {
  private final FlowNodeTransformer flowNodeTransformer = new FlowNodeTransformer();
  private final SequenceFlowTransformer sequenceFlowTransformer = new SequenceFlowTransformer();
  private final ServiceTaskTransformer serviceTaskTransformer = new ServiceTaskTransformer();
  private final ExclusiveGatewayTransformer exclusiveGatewayTransformer =
      new ExclusiveGatewayTransformer();

  public void transform(ErrorCollector errorCollector, ProcessImpl process) {
    final ParseContext context = new ParseContext(process);

    final Map<DirectBuffer, FlowElementImpl> flowElementsById = getFlowElementsById(context.getFlowElements());
    process.getFlowElementMap().putAll(flowElementsById);

    final List<FlowElementContainer> flowElementContainers = context.getElementsOfType(FlowElementContainer.class);

    for (FlowElementContainer container : flowElementContainers)
    {
      setInitialStartEvent(container);
    }

    final List<FlowNodeImpl> flowNodes = context.getElementsOfType(FlowNodeImpl.class);

    sequenceFlowTransformer.transform(errorCollector, context.getElementsOfType(SequenceFlowImpl.class), flowElementsById);
    flowNodeTransformer.transform(errorCollector, flowNodes);
    serviceTaskTransformer.transform(errorCollector, context.getElementsOfType(ServiceTaskImpl.class));
    exclusiveGatewayTransformer.transform(context.getElementsOfType(ExclusiveGatewayImpl.class));

    context.getElementsOfType(BoundaryEventImpl.class).forEach(e -> {
      final FlowNodeImpl attachedElement = (FlowNodeImpl) flowElementsById.get(e.getAttachedToRef());
      attachedElement.getBoundaryEvents().add(e);
    });

    transformBpmnAspects(context);
  }

  private Map<DirectBuffer, FlowElementImpl> getFlowElementsById(
      List<FlowElementImpl> flowElements) {
    final Map<DirectBuffer, FlowElementImpl> map = new HashMap<>();
    for (FlowElementImpl flowElement : flowElements) {
      map.put(flowElement.getIdAsBuffer(), flowElement);
    }
    return map;
  }

  private void setInitialStartEvent(final FlowElementContainer container) {
    final List<StartEventImpl> startEvents = container.getStartEvents();
    if (startEvents.size() >= 1) {
      final StartEventImpl startEvent = startEvents.get(0);
      container.setInitialStartEvent(startEvent);
    }
  }

  private void transformBpmnAspects(ParseContext context) {
    final List<FlowElementImpl> flowElements = context.getFlowElements();
    for (int f = 0; f < flowElements.size(); f++) {
      final FlowElementImpl flowElement = (FlowElementImpl) flowElements.get(f);

      if (flowElement instanceof FlowNode) {
        final FlowNode flowNode = (FlowNode) flowElement;

        final List<SequenceFlow> outgoingSequenceFlows = flowNode.getOutgoingSequenceFlows();
        if (outgoingSequenceFlows.isEmpty()) {
          if (flowElement instanceof SubProcessImpl && ((SubProcessImpl) flowElement).isTriggeredByEvent())
          {
            flowElement.setBpmnAspect(BpmnAspect.PARENT_TERMINATION);
          }
          else
          {
            flowElement.setBpmnAspect(BpmnAspect.SCOPE_MERGE);
          }
        } else if (outgoingSequenceFlows.size() == 1
            && !outgoingSequenceFlows.get(0).hasCondition()) {
          flowElement.setBpmnAspect(BpmnAspect.TAKE_SEQUENCE_FLOW);
        } else if (flowElement instanceof ExclusiveGateway) {
          flowElement.setBpmnAspect(BpmnAspect.EXCLUSIVE_SPLIT);
        }
        else if (flowElement instanceof ParallelGatewayImpl) {
          if (((ParallelGatewayImpl) flowElement).getOutgoing().size() == 1)
          {
            flowElement.setBpmnAspect(BpmnAspect.TAKE_SEQUENCE_FLOW);
          }
          else
          {
            flowElement.setBpmnAspect(BpmnAspect.PARALLEL_SPLIT);
          }
        }
      }
      else if (flowElement instanceof SequenceFlow)
      {
        final SequenceFlow flow = (SequenceFlow) flowElement;
        final FlowNode target = flow.getTargetNode();

        if (target instanceof ExclusiveGateway)
        {
          flowElement.setBpmnAspect(BpmnAspect.ACTIVATE_GATEWAY);
        }
        else if (target instanceof ParallelGatewayImpl)
        {
          if (target.getIncomingSequenceFlows().size() == 1)
          {
            flowElement.setBpmnAspect(BpmnAspect.ACTIVATE_GATEWAY);
          }
          else
          {
            flowElement.setBpmnAspect(BpmnAspect.PARALLEL_MERGE);
          }
        }
        else if (target instanceof EndEvent)
        {
          flowElement.setBpmnAspect(BpmnAspect.TRIGGER_NONE_EVENT);
        }
        else if (target instanceof ServiceTask || target instanceof SubProcessImpl)
        {
          flowElement.setBpmnAspect(BpmnAspect.START_ACTIVITY);
        }
      }

    }
  }

  private static class ParseContext
  {
    List<FlowElementImpl> flowElements = new ArrayList<>();

    ParseContext(ProcessImpl process)
    {
      this.flowElements.add(process);
      this.flowElements.addAll(process.collectFlowElements());
    }

    public <T extends FlowElementImpl> List<T> getElementsOfType(Class<T> type)
    {
      return flowElements.stream()
          .filter(e -> type.isAssignableFrom(e.getClass()))
          .map(type::cast)
          .collect(Collectors.toCollection(ArrayList::new));
    }

    public List<FlowElementImpl> getFlowElements() {
      return flowElements;
    }
  }
}
