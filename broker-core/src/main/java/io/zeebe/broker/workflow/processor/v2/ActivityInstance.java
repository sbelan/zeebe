package io.zeebe.broker.workflow.processor.v2;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import io.zeebe.broker.job.data.JobRecord;
import io.zeebe.broker.logstreams.processor.TypedRecord;

public class ActivityInstance {

  private int activeTokens;
  private UnsafeBuffer payload;

  public int getActiveTokens()
  {
    return activeTokens;
  }

  public void consumeTokens(int number)
  {
    activeTokens -= number;
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
