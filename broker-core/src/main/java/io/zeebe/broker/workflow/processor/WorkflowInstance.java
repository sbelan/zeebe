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

  public Scope newScope(long parentScopeKey, long scopeKey)
  {
    final Scope parentScope = scopes.get(parentScopeKey);
    // TODO: wire scopes
    final Scope newScope = new Scope(scopeKey);
    scopes.put(scopeKey, newScope);
    return newScope;
  }

  public void removeScope(long key)
  {
    scopes.remove(key);
    // TODO: wiring
  }
}
