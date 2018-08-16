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
package io.zeebe.gateway.impl.record;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.zeebe.gateway.api.events.JobEvent;
import io.zeebe.gateway.api.record.JobActivateResponseRecord;
import io.zeebe.gateway.impl.data.ZeebeObjectMapperImpl;
import io.zeebe.gateway.impl.event.JobActivateEventImpl;
import io.zeebe.gateway.impl.event.JobEventImpl;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.ValueType;
import java.util.List;

public class JobActivateResponseRecordImpl extends RecordImpl implements JobActivateResponseRecord {
  @JsonSerialize(contentAs = JobEventImpl.class)
  @JsonDeserialize(contentAs = JobEventImpl.class)
  private List<JobEvent> jobs;

  public JobActivateResponseRecordImpl(ZeebeObjectMapperImpl objectMapper, RecordType recordType) {
    super(objectMapper, recordType, ValueType.JOB_ACTIVATE_RESPONSE);
  }

  @Override
  public List<JobEvent> getJobs() {
    return jobs;
  }

  public void setJobs(List<JobEvent> jobs) {
    this.jobs = jobs;
  }

  @Override
  public Class<? extends RecordImpl> getEventClass() {
    return JobActivateEventImpl.class;
  }
}
