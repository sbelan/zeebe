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
package io.zeebe.gateway.impl.event;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.zeebe.gateway.api.events.JobActivateEvent;
import io.zeebe.gateway.impl.data.ZeebeObjectMapperImpl;
import io.zeebe.gateway.impl.record.JobActivateResponseRecordImpl;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.intent.JobActivateResponseIntent;

public class JobActivateEventImpl extends JobActivateResponseRecordImpl
    implements JobActivateEvent {
  @JsonCreator
  public JobActivateEventImpl(@JacksonInject ZeebeObjectMapperImpl mapper) {
    super(mapper, RecordType.EVENT);
    setIntent(JobActivateResponseIntent.ACTIVATED);
  }
}
