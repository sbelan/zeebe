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

import io.zeebe.gateway.api.record.JobActivateRequestRecord;
import io.zeebe.gateway.impl.data.ZeebeObjectMapperImpl;
import io.zeebe.gateway.impl.event.JobActivateEventImpl;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.ValueType;

public class JobActivateRequestRecordImpl extends RecordImpl implements JobActivateRequestRecord {

  private String jobType;
  private int amount;
  private String workerName;
  private long timeout;

  public JobActivateRequestRecordImpl(ZeebeObjectMapperImpl objectMapper, RecordType recordType) {
    super(objectMapper, recordType, ValueType.JOB_ACTIVATE_REQUEST);
  }

  @Override
  public String getWorkerName() {
    return workerName;
  }

  public void setWorkerName(String workerName) {
    this.workerName = workerName;
  }

  @Override
  public long getTimeout() {
    return timeout;
  }

  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }

  @Override
  public String getJobType() {
    return jobType;
  }

  public void setJobType(String jobType) {
    this.jobType = jobType;
  }

  @Override
  public int getAmount() {
    return amount;
  }

  public void setAmount(int amount) {
    this.amount = amount;
  }

  @Override
  public Class<? extends RecordImpl> getEventClass() {
    return JobActivateEventImpl.class;
  }
}
