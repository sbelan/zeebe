package io.zeebe.client.impl;

import io.zeebe.client.api.commands.JobInfo;
import io.zeebe.client.api.events.ActivateJobResponse;
import java.util.ArrayList;
import java.util.List;

public class ActivateJobResponseImpl implements ActivateJobResponse {
  private List<JobInfo> jobs;

  public ActivateJobResponseImpl(
      final io.zeebe.gateway.protocol.GatewayOuterClass.ActivateJobResponse response) {
    jobs = new ArrayList<>();
    for (final io.zeebe.gateway.protocol.GatewayOuterClass.JobInfo job : response.getJobsList()) {
      jobs.add(new JobInfoImpl(job));
    }
  }

  @Override
  public List<JobInfo> getJobs() {
    return jobs;
  }
}
