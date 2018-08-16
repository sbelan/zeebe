package io.zeebe.client.api.events;

import io.zeebe.client.api.commands.JobInfo;
import java.util.List;

public interface ActivateJobResponse {
  List<JobInfo> getJobs();
}
