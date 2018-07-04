package io.zeebe.broker.workflow.processor.v2;

import org.agrona.collections.Long2ObjectHashMap;

public class WorkflowInstances {

  private Long2ObjectHashMap<WorkflowInstance> instances = new Long2ObjectHashMap<>();

  public WorkflowInstance getWorkflowInstance(long key)
  {
    return instances.get(key);
  }

  public WorkflowInstance newWorkflowInstance(long key)
  {
    final WorkflowInstance instance = new WorkflowInstance(key);
    instances.put(key, instance);
    return instance;
  }
}
