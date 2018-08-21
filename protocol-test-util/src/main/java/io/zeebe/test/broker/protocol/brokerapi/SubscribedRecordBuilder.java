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
package io.zeebe.test.broker.protocol.brokerapi;

import io.zeebe.protocol.clientapi.MessageHeaderEncoder;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.clientapi.SubscribedRecordEncoder;
import io.zeebe.protocol.clientapi.SubscriptionType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.Intent;
import io.zeebe.test.broker.protocol.MsgPackHelper;
import io.zeebe.test.util.collection.MapBuilder;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.ServerTransport;
import io.zeebe.util.buffer.BufferWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import org.agrona.MutableDirectBuffer;

public class SubscribedRecordBuilder implements BufferWriter {
  protected final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  protected final SubscribedRecordEncoder bodyEncoder = new SubscribedRecordEncoder();

  protected final MsgPackHelper msgPackHelper;
  protected final ServerTransport transport;

  protected int partitionId;
  protected long position;
  private long sourceRecordPosition;
  protected long key;
  protected long subscriberKey;
  protected SubscriptionType subscriptionType;
  private RecordType recordType;
  protected ValueType valueType;
  private Intent intent;
  protected byte[] event;
  protected Instant timestamp = Instant.ofEpochMilli(0);
  private RejectionType rejectionType = RejectionType.NULL_VAL;
  private byte[] rejectionReason = new byte[0];

  public SubscribedRecordBuilder(MsgPackHelper msgPackHelper, ServerTransport transport) {
    this.msgPackHelper = msgPackHelper;
    this.transport = transport;
  }

  public SubscribedRecordBuilder partitionId(int partitionId) {
    this.partitionId = partitionId;
    return this;
  }

  public SubscribedRecordBuilder position(long position) {
    this.position = position;
    return this;
  }

  public SubscribedRecordBuilder sourceRecordPosition(long sourceRecordPosition) {
    this.sourceRecordPosition = sourceRecordPosition;
    return this;
  }

  public SubscribedRecordBuilder key(long key) {
    this.key = key;
    return this;
  }

  public SubscribedRecordBuilder subscriberKey(long subscriberKey) {
    this.subscriberKey = subscriberKey;
    return this;
  }

  public SubscribedRecordBuilder subscriptionType(SubscriptionType subscriptionType) {
    this.subscriptionType = subscriptionType;
    return this;
  }

  public SubscribedRecordBuilder recordType(RecordType recordType) {
    this.recordType = recordType;
    return this;
  }

  public SubscribedRecordBuilder intent(Intent intent) {
    this.intent = intent;
    return this;
  }

  public SubscribedRecordBuilder valueType(ValueType valueType) {
    this.valueType = valueType;
    return this;
  }

  public SubscribedRecordBuilder value(Map<String, Object> event) {
    this.event = msgPackHelper.encodeAsMsgPack(event);
    return this;
  }

  public MapBuilder<SubscribedRecordBuilder> value() {
    return new MapBuilder<>(this, this::value);
  }

  public SubscribedRecordBuilder timestamp(Instant timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  public SubscribedRecordBuilder rejectionType(RejectionType rejectionType) {
    this.rejectionType = rejectionType;
    return this;
  }

  public SubscribedRecordBuilder rejectionReason(String rejectionReason) {
    this.rejectionReason = rejectionReason.getBytes(StandardCharsets.UTF_8);
    return this;
  }

  public void push(RemoteAddress target) {
    final boolean success = transport.getOutput().sendMessage(target.getStreamId(), this);

    if (!success) {
      throw new RuntimeException("Could not schedule message on send buffer");
    }
  }

  @Override
  public int getLength() {
    return MessageHeaderEncoder.ENCODED_LENGTH
        + SubscribedRecordEncoder.BLOCK_LENGTH
        + SubscribedRecordEncoder.valueHeaderLength()
        + event.length
        + SubscribedRecordEncoder.rejectionReasonHeaderLength()
        + rejectionReason.length;
  }

  @Override
  public void write(MutableDirectBuffer buffer, int offset) {
    headerEncoder
        .wrap(buffer, offset)
        .blockLength(bodyEncoder.sbeBlockLength())
        .schemaId(bodyEncoder.sbeSchemaId())
        .templateId(bodyEncoder.sbeTemplateId())
        .version(bodyEncoder.sbeSchemaVersion());

    bodyEncoder
        .wrap(buffer, offset + headerEncoder.encodedLength())
        .recordType(recordType)
        .valueType(valueType)
        .intent(intent.value())
        .key(key)
        .position(position)
        .sourceRecordPosition(sourceRecordPosition)
        .subscriberKey(subscriberKey)
        .subscriptionType(subscriptionType)
        .partitionId(partitionId)
        .timestamp(timestamp.toEpochMilli())
        .rejectionType(rejectionType)
        .putValue(event, 0, event.length)
        .putRejectionReason(rejectionReason, 0, rejectionReason.length);
  }
}
