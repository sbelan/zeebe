package io.zeebe.broker.workflow.processor;

import org.agrona.collections.Long2ObjectHashMap;

public class WorkflowInstance {

  private Long2ObjectHashMap<ScopeInstance> scopes = new Long2ObjectHashMap<>();
  private ScopeInstance rootScope;

  public WorkflowInstance(long position, long key)
  {
    this.rootScope = new ScopeInstance(key, -1, position);
    scopes.put(key, rootScope);
  }

  public ScopeInstance getScope(long scopeKey)
  {
    return scopes.get(scopeKey);
  }

  public ScopeInstance newScope(long position, long parentScopeKey, long scopeKey)
  {
    final ScopeInstance parentScope = scopes.get(parentScopeKey);
    final ScopeInstance newScope = new ScopeInstance(scopeKey, parentScopeKey, position);
    scopes.put(scopeKey, newScope);
    parentScope.addChildScope(newScope);
    return newScope;
  }

  public void removeScope(long key)
  {
    final ScopeInstance scope = scopes.remove(key);

    if (scope.getParentKey() >= 0)
    {
      final ScopeInstance parentScope = scopes.get(scope.getParentKey());
      parentScope.removeChildScope(scope);
    }
  }
}
