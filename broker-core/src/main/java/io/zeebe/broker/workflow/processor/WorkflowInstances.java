package io.zeebe.broker.workflow.processor;

import org.agrona.collections.Long2ObjectHashMap;

public class WorkflowInstances {

  private Long2ObjectHashMap<WorkflowInstance> workflowInstances = new Long2ObjectHashMap<>();

  public void onWorkflowInstanceCreated(long position, long key)
  {
    workflowInstances.put(key, new WorkflowInstance(position, key));
  }

  public WorkflowInstance get(long key)
  {
    return workflowInstances.get(key);
  }

  public void onWorkflowInstanceFinished(long key)
  {
    workflowInstances.remove(key);
  }
}
