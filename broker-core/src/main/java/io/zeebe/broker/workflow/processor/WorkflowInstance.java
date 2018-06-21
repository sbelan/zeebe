package io.zeebe.broker.workflow.processor;

import org.agrona.collections.Long2ObjectHashMap;

public class WorkflowInstance {

  private Long2ObjectHashMap<Scope> scopes = new Long2ObjectHashMap<>();
  private Scope rootScope;

  public WorkflowInstance(long position, long key)
  {
    this.rootScope = new Scope(key, -1, position);
    scopes.put(key, rootScope);
  }

  public Scope getScope(long scopeKey)
  {
    return scopes.get(scopeKey);
  }

  public Scope newScope(long position, long parentScopeKey, long scopeKey)
  {
    final Scope parentScope = scopes.get(parentScopeKey);
    final Scope newScope = new Scope(scopeKey, parentScopeKey, position);
    scopes.put(scopeKey, newScope);
    parentScope.addChildScope(newScope);
    return newScope;
  }

  public void removeScope(long key)
  {
    final Scope scope = scopes.remove(key);

    if (scope.getParentKey() >= 0)
    {
      final Scope parentScope = scopes.get(scope.getParentKey());
      parentScope.removeChildScope(scope);
    }
  }
}
