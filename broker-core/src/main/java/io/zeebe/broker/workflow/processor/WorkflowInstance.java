package io.zeebe.broker.workflow.processor;

import org.agrona.collections.Long2ObjectHashMap;

public class WorkflowInstance {

  private Long2ObjectHashMap<Scope> scopes = new Long2ObjectHashMap<>();
  private Scope rootScope;

  public WorkflowInstance(long key)
  {
    this.rootScope = new Scope(key);
    scopes.put(key, rootScope);
  }

  public Scope getScope(long scopeKey)
  {
    return scopes.get(scopeKey);
  }
}
