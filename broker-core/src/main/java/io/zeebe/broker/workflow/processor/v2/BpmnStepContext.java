package io.zeebe.broker.workflow.processor.v2;

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.model.bpmn.instance.FlowElement;
import io.zeebe.model.bpmn.instance.Workflow;
import io.zeebe.model.bpmn.instance.WorkflowDefinition;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class BpmnStepContext<T extends FlowElement> {

  private FlowElement element;
  private ActivityInstance activityInstance;
  private Workflow workflowDefinition;
  private WorkflowInstance workflowInstance;
  private TypedRecord<WorkflowInstanceRecord> currentRecord;
  private Lifecycle<WorkflowInstanceIntent, WorkflowInstanceRecord> wfLifecycle;

  public ActivityInstance getActivityInstance()
  {
    return activityInstance;
  }

  public void setActivityInstance(ActivityInstance activityInstance) {
    this.activityInstance = activityInstance;
  }

  public T getElement()
  {
    return (T) element;
  }

  public void setElement(FlowElement element) {
    this.element = element;
  }

  public Workflow getWorkflow()
  {
    return workflowDefinition;
  }

  public void setWorkflow(Workflow workflowDefinition) {
    this.workflowDefinition = workflowDefinition;
  }

  public WorkflowInstance getWorkflowInstance()
  {
    return workflowInstance;
  }

  public void setWorkflowInstance(WorkflowInstance workflowInstance) {
    this.workflowInstance = workflowInstance;
  }

  public TypedRecord<WorkflowInstanceRecord> getCurrentRecord()
  {
    return currentRecord;
  }

  public void setCurrentRecord(TypedRecord<WorkflowInstanceRecord> currentRecord) {
    this.currentRecord = currentRecord;
  }

  public WorkflowInstanceRecord getCurrentValue()
  {
    return currentRecord.getValue();
  }

  public Lifecycle<WorkflowInstanceIntent, WorkflowInstanceRecord> getRecordWriter() {
    return lifecycle;
  }
}
