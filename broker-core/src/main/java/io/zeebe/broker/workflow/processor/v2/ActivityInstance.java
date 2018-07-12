package io.zeebe.broker.workflow.processor.v2;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import io.zeebe.broker.job.data.JobRecord;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;

public class ActivityInstance {

  private int activeTokens = 0;
  private UnsafeBuffer payload;
  private TypedRecord<WorkflowInstanceRecord> latestState;

  public int getActiveTokens()
  {
    return activeTokens;
  }

  public void consumeTokens(int number)
  {
    activeTokens -= number;
  }

  public void spawnTokens(int number)
  {
    activeTokens += number;
  }

  public void setLatestState(TypedRecord<WorkflowInstanceRecord> state)
  {
    // TODO: copy record
    this.latestState = state;
  }

  // TODO: could also simply make copies of the entire TypedRecord, if we want to access all properties anyway
  public void setPayload(DirectBuffer payload) {
    // TODO: could also use expandablearraybuffer or similar
    this.payload = new UnsafeBuffer(new byte[payload.capacity()]);
    this.payload.putBytes(0, payload, 0, payload.capacity());

  }

  public DirectBuffer getPayload()
  {
    return payload;
  }

  public void addJob(TypedRecord<JobRecord> record) {
    // TODO Auto-generated method stub

  }

  public void removeJob(TypedRecord<JobRecord> record) {
    // TODO Auto-generated method stub

  }
}
