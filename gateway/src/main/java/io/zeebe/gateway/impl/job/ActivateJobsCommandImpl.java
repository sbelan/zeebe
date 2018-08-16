/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.gateway.impl.job;

import io.zeebe.gateway.api.commands.ActivateJobsCommandStep1;
import io.zeebe.gateway.api.events.JobActivateEvent;
import io.zeebe.gateway.impl.CommandImpl;
import io.zeebe.gateway.impl.RequestManager;
import io.zeebe.gateway.impl.command.JobActivateCommandImpl;
import io.zeebe.gateway.impl.data.ZeebeObjectMapperImpl;
import io.zeebe.gateway.impl.record.RecordImpl;

public class ActivateJobsCommandImpl extends CommandImpl<JobActivateEvent>
    implements ActivateJobsCommandStep1 {

  private final JobActivateCommandImpl command;

  public ActivateJobsCommandImpl(
      RequestManager commandManager, ZeebeObjectMapperImpl objectMapper, String topic) {
    super(commandManager);

    command = new JobActivateCommandImpl(objectMapper);
    command.setTopicName(topic);
  }

  @Override
  public ActivateJobsCommandStep1 jobType(String jobType) {
    command.setJobType(jobType);
    return this;
  }

  @Override
  public ActivateJobsCommandStep1 amount(int amount) {
    command.setAmount(amount);
    return this;
  }

  @Override
  public ActivateJobsCommandStep1 workerName(String workerName) {
    command.setWorkerName(workerName);
    return this;
  }

  @Override
  public ActivateJobsCommandStep1 timeout(long timeout) {
    command.setTimeout(timeout);
    return this;
  }

  @Override
  public RecordImpl getCommand() {
    return command;
  }
}
