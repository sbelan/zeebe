package io.zeebe.broker.workflow.processor.v2;

public class KeyGenerator {

  private long key = 1;

  public long nextKey()
  {
    return key++;
  }
}
