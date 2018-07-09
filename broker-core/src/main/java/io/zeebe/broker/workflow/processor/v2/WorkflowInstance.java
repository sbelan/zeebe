package io.zeebe.broker.workflow.processor.v2;

import org.agrona.collections.Long2ObjectHashMap;

public class WorkflowInstance {

  private ActivityInstance rootScope;
  private Long2ObjectHashMap<ActivityInstance> activityInstances = new Long2ObjectHashMap<>();

  public WorkflowInstance(long key)
  {
    this.rootScope = newActivityInstance(key);
  }

  public ActivityInstance getRootScope()
  {
    return rootScope;
  }

  public ActivityInstance newActivityInstance(long key)
  {
    final ActivityInstance instance = new ActivityInstance();
    activityInstances.put(key, instance);
    return instance;
  }

  public ActivityInstance getActivityInstance(long activityInstanceKey) {
    return activityInstances.get(activityInstanceKey);

  }

  public void removeActivityInstance(long key) {
    // TODO Auto-generated method stub

  }
}
