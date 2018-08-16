package io.zeebe.client.impl;

import io.zeebe.client.api.ZeebeFuture;
import io.zeebe.client.api.commands.ActivateJobsRequestStep1;
import io.zeebe.client.api.events.ActivateJobResponse;
import io.zeebe.gateway.protocol.GatewayGrpc.GatewayStub;
import io.zeebe.gateway.protocol.GatewayOuterClass.ActivateJobRequest;
import io.zeebe.util.EnsureUtil;

public class ActivateJobRequestImpl implements ActivateJobsRequestStep1 {
  private final GatewayStub asyncStub;

  private String topicName;
  private String jobType;
  private String workerName;
  private long timeout;
  private int amount;

  ActivateJobRequestImpl(final GatewayStub asyncStub) {
    this.asyncStub = asyncStub;
  }

  @Override
  public ZeebeFuture<ActivateJobResponse> send() {
    EnsureUtil.ensureNotNullOrEmpty("topicName", topicName);
    EnsureUtil.ensureNotNullOrEmpty("jobType", jobType);
    EnsureUtil.ensureNotNullOrEmpty("workerName", workerName);
    EnsureUtil.ensureGreaterThanOrEqual("amount", amount, 1);

    final ActivateJobRequest request =
        ActivateJobRequest.newBuilder()
            .setAmount(amount)
            .setTopicName(topicName)
            .setType(jobType)
            .setWorker(workerName)
            .build();

    final ZeebeClientFutureImpl<
            ActivateJobResponse, io.zeebe.gateway.protocol.GatewayOuterClass.ActivateJobResponse>
        future = new ZeebeClientFutureImpl<>(ActivateJobResponseImpl::new);

    asyncStub.activateJobs(request, future);

    return future;
  }

  @Override
  public ActivateJobsRequestStep1 topicName(String topicName) {
    this.topicName = topicName;
    return this;
  }

  @Override
  public ActivateJobsRequestStep1 workerName(String workerName) {
    this.workerName = workerName;
    return this;
  }

  @Override
  public ActivateJobsRequestStep1 jobType(String jobType) {
    this.jobType = jobType;
    return this;
  }

  @Override
  public ActivateJobsRequestStep1 timeout(long timeout) {
    this.timeout = timeout;
    return this;
  }

  @Override
  public ActivateJobsRequestStep1 amount(int amount) {
    this.amount = amount;
    return this;
  }
}
