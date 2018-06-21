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
package io.zeebe.client.impl.command;

import com.fasterxml.jackson.annotation.*;
import io.zeebe.client.api.commands.MessageSubscriptionCommandName;
import io.zeebe.client.impl.data.ZeebeObjectMapperImpl;
import io.zeebe.client.impl.record.MessageSubscriptionRecordImpl;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.intent.MessageSubscriptionIntent;

public class MessageSubscriptionCommandImpl extends MessageSubscriptionRecordImpl {

  @JsonCreator
  public MessageSubscriptionCommandImpl(@JacksonInject ZeebeObjectMapperImpl objectMapper) {
    super(objectMapper, RecordType.COMMAND);
  }

  public MessageSubscriptionCommandImpl(ZeebeObjectMapperImpl objectMapper, MessageSubscriptionIntent intent) {
    super(objectMapper, RecordType.COMMAND);
    setIntent(intent);
  }

  @JsonIgnore
  public MessageSubscriptionCommandName getName() {
    return MessageSubscriptionCommandName.valueOf(getMetadata().getIntent());
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("MessageSubscription [command=");
    builder.append(getName());
    builder.append(", messageName=");
    builder.append(getMessageName());
    builder.append(", messageKey=");
    builder.append(getMessageKey());
    builder.append("]");
    return builder.toString();
  }
}
