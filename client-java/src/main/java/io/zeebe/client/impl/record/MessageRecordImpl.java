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
package io.zeebe.client.impl.record;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.zeebe.client.api.record.MessageRecord;
import io.zeebe.client.impl.data.PayloadField;
import io.zeebe.client.impl.data.ZeebeObjectMapperImpl;
import io.zeebe.client.impl.event.MessageEventImpl;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.MessageIntent;
import java.io.InputStream;
import java.util.Map;

public abstract class MessageRecordImpl extends RecordImpl implements MessageRecord {

  private String messageName;
  private String messageKey;
  private PayloadField payload;

  public MessageRecordImpl(ZeebeObjectMapperImpl objectMapper, RecordType recordType) {
    super(objectMapper, recordType, ValueType.MESSAGE);
  }

  public MessageRecordImpl(MessageRecordImpl base, MessageIntent intent) {
    super(base, intent);

    this.messageName = base.messageName;
    this.messageKey = base.messageKey;

    if (base.payload != null) {
      this.payload = new PayloadField(base.payload);
    }
  }

  @Override
  public String getMessageName() {
    return messageName;
  }

  public void setMessageName(String messageName) {
    this.messageName = messageName;
  }

  @Override
  public String getMessageKey() {
    return messageKey;
  }

  public void setMessageKey(String messageKey) {
    this.messageKey = messageKey;
  }

  @JsonProperty("payload")
  public PayloadField getPayloadField() {
    return payload;
  }

  @JsonProperty("payload")
  public void setPayloadField(PayloadField payload) {
    this.payload = payload;
  }

  @Override
  public String getPayload() {
    if (payload == null) {
      return null;
    } else {
      return payload.getAsJsonString();
    }
  }

  @JsonIgnore
  @Override
  public Map<String, Object> getPayloadAsMap() {
    if (payload == null) {
      return null;
    } else {
      return payload.getAsMap();
    }
  }

  public void setPayload(String jsonString) {
    initializePayloadField();
    this.payload.setJson(jsonString);
  }

  public void setPayload(InputStream jsonStream) {
    initializePayloadField();
    this.payload.setJson(jsonStream);
  }

  public void setPayload(Map<String, Object> payload) {
    initializePayloadField();
    this.payload.setAsMap(payload);
  }

  private void initializePayloadField() {
    if (payload == null) {
      payload = new PayloadField(objectMapper);
    }
  }

  public void clearPayload() {
    // set field to null so that it is not serialized to Msgpack
    // - currently, the broker doesn't support null as payload
    payload = null;
  }

  @Override
  public Class<? extends RecordImpl> getEventClass() {
    return MessageEventImpl.class;
  }
}
