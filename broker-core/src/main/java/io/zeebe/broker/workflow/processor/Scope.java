package io.zeebe.broker.workflow.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import io.zeebe.util.CollectionUtil;
import io.zeebe.util.buffer.BufferUtil;

public class Scope {

  private long key;

  /*
   * purpose:
   *   - find tokens that can be joined on parallel gw or scope completion
   *   - find payload that must be merged in these cases
   */
  // TODO: naming
  private Map<String, List<Long>> suspendedTokens = new HashMap<>();
  private int activeTokens = 0;

  /*
   * purpose:
   *   - decide if scope can be terminated
   */
  private ScopeState state;

  /*
   * purpose:
   *   - create a CANCEL job command on activity instance cancellation
   */
  private Long2ObjectHashMap<Job> jobs = new Long2ObjectHashMap<>();

  /*
   * purpose:
   *   - subprocess completion
   */
  private final long parentKey;

  /*
   * purpose:
   *   - top-down propagation of termination
   */
  private List<Scope> childScopes = new ArrayList<>();

  /*
   * purpose:
   *   - write TERMINATING events based on last value
   *
   * (TODO: currently never updated, because we don't need to)
   */
  private long position;

  public Scope(long key, long parentKey, long position)
  {
    this.key = key;
    this.parentKey = parentKey;
    this.state = ScopeState.ACTIVE;
    this.position = position;
  }


  // TODO: methods to index something because it is interruptible

  public void onJobCreated(long key, long position)
  {
    final Job job = new Job();
    job.setPosition(position);
    jobs.put(key, job);
  }

  // canceled or completed
  public void onJobFinished(long key)
  {
    jobs.remove(key);
  }

  public void consumeTokens(int i)
  {
    activeTokens -= i;
  }

  public void spawnTokens(int i)
  {
    activeTokens += i;
  }

  public void suspendToken(DirectBuffer elementId, long position)
  {
    CollectionUtil.addToMapOfLists(suspendedTokens, BufferUtil.bufferAsString(elementId), position);
  }

  public long getSuspendedToken(DirectBuffer elementId)
  {
    final String stringElementId = BufferUtil.bufferAsString(elementId);

    if (suspendedTokens.containsKey(stringElementId))
    {
      final List<Long> suspendedTokensAtElement = suspendedTokens.get(stringElementId);
      if (!suspendedTokensAtElement.isEmpty())
      {
        return suspendedTokensAtElement.get(0);
      }
    }

    return -1;
  }

  public List<Long> getSuspendedTokens()
  {
    final List<Long> result = new ArrayList<>();
    this.suspendedTokens.values().forEach(result::addAll);

    return result;
  }

  public void consumeSuspendedToken(long position)
  {
    suspendedTokens.forEach((element, tokens) -> {
      if (tokens.contains(position))
      {
        tokens.remove(position);
      }
    });
  }

  public int getActiveTokens() {
    return activeTokens;
  }

  public long getParentKey() {
    return parentKey;
  }

  public long getKey() {
    return key;
  }

  public ScopeState getState() {
    return state;
  }

  public void setState(ScopeState state) {
    this.state = state;
  }

  public List<Scope> getChildScopes() {
    return childScopes;
  }

  public void addChildScope(Scope scope)
  {
    childScopes.add(scope);
  }

  public void removeChildScope(Scope scope)
  {
    childScopes.remove(scope);
  }

  public long getPosition() {
    return position;
  }

  public void setPosition(long position) {
    this.position = position;
  }

  public enum ScopeState
  {
    TERMINATING,
    ACTIVE // Not terminating
  }

}
