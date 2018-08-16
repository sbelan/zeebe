package io.zeebe.client.api.commands;

import io.zeebe.client.api.events.ActivateJobResponse;

public interface ActivateJobsRequestStep1 extends FinalCommandStep<ActivateJobResponse> {
  ActivateJobsRequestStep1 topicName(String topicName);

  ActivateJobsRequestStep1 workerName(String workerName);

  ActivateJobsRequestStep1 jobType(String jobType);

  ActivateJobsRequestStep1 timeout(long timeout);

  ActivateJobsRequestStep1 amount(int amount);
}
