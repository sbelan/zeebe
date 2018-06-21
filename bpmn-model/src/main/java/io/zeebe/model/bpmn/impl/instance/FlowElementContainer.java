package io.zeebe.model.bpmn.impl.instance;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;
import io.zeebe.model.bpmn.BpmnConstants;
import io.zeebe.model.bpmn.instance.FlowElement;
import io.zeebe.model.bpmn.instance.StartEvent;

// TODO: this inheritance hierarchy is not correct for ProcessImpl (=> no flow node)
public class FlowElementContainer extends FlowNodeImpl {

  private List<SequenceFlowImpl> sequenceFlows = new ArrayList<>();
  private List<StartEventImpl> startEvents = new ArrayList<>();
  private List<EndEventImpl> endEvents = new ArrayList<>();
  private List<ServiceTaskImpl> serviceTasks = new ArrayList<>();
  private List<ExclusiveGatewayImpl> exclusiveGateways = new ArrayList<>();
  private List<ParallelGatewayImpl> parallelGateways = new ArrayList<>();
  private List<SubProcessImpl> subprocesses = new ArrayList<>();
  private List<BoundaryEventImpl> boundaryEvents = new ArrayList<>();

  private StartEvent initialStartEvent;


  @XmlElement(name = BpmnConstants.BPMN_ELEMENT_SEQUENCE_FLOW, namespace = BpmnConstants.BPMN20_NS)
  public void setSequenceFlows(List<SequenceFlowImpl> sequenceFlows) {
    this.sequenceFlows = sequenceFlows;
  }

  public List<SequenceFlowImpl> getSequenceFlows() {
    return sequenceFlows;
  }

  @XmlElement(name = BpmnConstants.BPMN_ELEMENT_START_EVENT, namespace = BpmnConstants.BPMN20_NS)
  public void setStartEvents(List<StartEventImpl> startEvents) {
    this.startEvents = startEvents;
  }

  public List<StartEventImpl> getStartEvents() {
    return startEvents;
  }

  @XmlElement(name = BpmnConstants.BPMN_ELEMENT_END_EVENT, namespace = BpmnConstants.BPMN20_NS)
  public void setEndEvents(List<EndEventImpl> endEvents) {
    this.endEvents = endEvents;
  }

  public List<EndEventImpl> getEndEvents() {
    return endEvents;
  }

  @XmlElement(name = BpmnConstants.BPMN_ELEMENT_SERVICE_TASK, namespace = BpmnConstants.BPMN20_NS)
  public void setServiceTasks(List<ServiceTaskImpl> serviceTasks) {
    this.serviceTasks = serviceTasks;
  }

  public List<ServiceTaskImpl> getServiceTasks() {
    return serviceTasks;
  }

  public List<SubProcessImpl> getSubprocesses() {
    return subprocesses;
  }

  @XmlElement(name = "subProcess", namespace = BpmnConstants.BPMN20_NS)
  public void setSubprocesses(List<SubProcessImpl> subprocesses) {
    this.subprocesses = subprocesses;
  }

  @XmlElement(
      name = BpmnConstants.BPMN_ELEMENT_EXCLUSIVE_GATEWAY,
      namespace = BpmnConstants.BPMN20_NS)
  public void setExclusiveGateways(List<ExclusiveGatewayImpl> exclusiveGateways) {
    this.exclusiveGateways = exclusiveGateways;
  }

  public List<ExclusiveGatewayImpl> getExclusiveGateways() {
    return exclusiveGateways;
  }

  @XmlElement(
      name = BpmnConstants.BPMN_ELEMENT_PARALLEL_GATEWAY,
      namespace = BpmnConstants.BPMN20_NS)
  public void setParallelGateways(List<ParallelGatewayImpl> parallelGateways) {
    this.parallelGateways = parallelGateways;
  }

  public List<ParallelGatewayImpl> getParallelGateways() {
    return parallelGateways;
  }

  @XmlElement(
      name = "boundaryEvent",
      namespace = BpmnConstants.BPMN20_NS)
  public void setBoundaryEvents(List<BoundaryEventImpl> boundaryEvents) {
    this.boundaryEvents = boundaryEvents;
  }

  public List<BoundaryEventImpl> getBoundaryEvents() {
    return boundaryEvents;
  }

  @XmlTransient
  public void setInitialStartEvent(StartEvent initialStartEvent) {
    this.initialStartEvent = initialStartEvent;
  }

  public StartEvent getInitialStartEvent() {
    return initialStartEvent;
  }

  public List<FlowElementImpl> collectFlowElements() {
    final List<FlowElementImpl> flowElements = new ArrayList<>();
    flowElements.addAll(startEvents);
    flowElements.addAll(endEvents);
    flowElements.addAll(sequenceFlows);
    flowElements.addAll(serviceTasks);
    flowElements.addAll(exclusiveGateways);
    flowElements.addAll(parallelGateways);
    flowElements.addAll(subprocesses);
    flowElements.addAll(boundaryEvents);
    subprocesses.forEach(s -> flowElements.addAll(s.collectFlowElements()));

    return flowElements;
  }

}
