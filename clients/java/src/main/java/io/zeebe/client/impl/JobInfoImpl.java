package io.zeebe.client.impl;

import io.zeebe.client.api.commands.JobInfo;

public class JobInfoImpl implements JobInfo {
  private long key;

  public JobInfoImpl(final io.zeebe.gateway.protocol.GatewayOuterClass.JobInfo job) {
    serialize(job);
  }

  private void serialize(final io.zeebe.gateway.protocol.GatewayOuterClass.JobInfo job) {
    this.key = job.getKey();
  }

  @Override
  public long getKey() {
    return key;
  }
}
