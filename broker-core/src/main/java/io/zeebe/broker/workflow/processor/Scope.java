package io.zeebe.broker.workflow.processor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.agrona.DirectBuffer;
import io.zeebe.util.CollectionUtil;
import io.zeebe.util.buffer.BufferUtil;

public class Scope {

  private long key;

  private Map<String, List<Long>> suspendedTokens = new HashMap<>();
  private int activeTokens;

  public Scope(long key)
  {
    this.key = key;
  }


  // TODO: methods to index something because it is interruptible

  public void consumeTokens(int i)
  {

  }

  public void spawnTokens(int i)
  {

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

  public void consumeSuspendedToken(long position)
  {
    suspendedTokens.forEach((element, tokens) -> {
      if (tokens.contains(position))
      {
        tokens.remove(position);
      }
    });
  }

}
