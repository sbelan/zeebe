/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.transport.clientapi;

import static io.zeebe.protocol.clientapi.SubscribedRecordEncoder.keyNullValue;
import static io.zeebe.protocol.clientapi.SubscribedRecordEncoder.partitionIdNullValue;
import static io.zeebe.protocol.clientapi.SubscribedRecordEncoder.positionNullValue;
import static io.zeebe.protocol.clientapi.SubscribedRecordEncoder.sourceRecordPositionNullValue;
import static io.zeebe.protocol.clientapi.SubscribedRecordEncoder.subscriberKeyNullValue;
import static io.zeebe.protocol.clientapi.SubscribedRecordEncoder.timestampNullValue;

import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.clientapi.MessageHeaderEncoder;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.clientapi.SubscribedRecordEncoder;
import io.zeebe.protocol.clientapi.SubscriptionType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.Intent;
import io.zeebe.transport.ServerOutput;
import io.zeebe.util.buffer.BufferWriter;
import io.zeebe.util.buffer.DirectBufferWriter;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class SubscribedRecordWriter implements BufferWriter {
  protected final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  protected final SubscribedRecordEncoder bodyEncoder = new SubscribedRecordEncoder();

  protected int partitionId = partitionIdNullValue();
  protected long position = positionNullValue();
  protected long sourceRecordPosition = sourceRecordPositionNullValue();
  protected long key = keyNullValue();
  protected long subscriberKey = subscriberKeyNullValue();
  protected SubscriptionType subscriptionType;
  protected ValueType valueType;
  protected DirectBufferWriter valueBuffer = new DirectBufferWriter();
  protected BufferWriter valueWriter;
  private RecordType recordType = RecordType.NULL_VAL;
  private short intent = Intent.NULL_VAL;
  protected long timestamp = timestampNullValue();
  private RejectionType rejectionType = RejectionType.NULL_VAL;
  private UnsafeBuffer rejectionReason = new UnsafeBuffer(0, 0);

  protected final ServerOutput output;

  public SubscribedRecordWriter(final ServerOutput output) {
    this.output = output;
  }

  public SubscribedRecordWriter partitionId(final int partitionId) {
    this.partitionId = partitionId;
    return this;
  }

  public SubscribedRecordWriter position(final long position) {
    this.position = position;
    return this;
  }

  public SubscribedRecordWriter sourceRecordPosition(long sourceRecordPosition) {
    this.sourceRecordPosition = sourceRecordPosition;
    return this;
  }

  public SubscribedRecordWriter key(final long key) {
    this.key = key;
    return this;
  }

  public SubscribedRecordWriter subscriberKey(final long subscriberKey) {
    this.subscriberKey = subscriberKey;
    return this;
  }

  public SubscribedRecordWriter subscriptionType(final SubscriptionType subscriptionType) {
    this.subscriptionType = subscriptionType;
    return this;
  }

  public SubscribedRecordWriter valueType(final ValueType valueType) {
    this.valueType = valueType;
    return this;
  }

  public SubscribedRecordWriter recordType(RecordType recordType) {
    this.recordType = recordType;
    return this;
  }

  public SubscribedRecordWriter intent(Intent intent) {
    this.intent = intent.value();
    return this;
  }

  public SubscribedRecordWriter value(
      final DirectBuffer buffer, final int offset, final int length) {
    this.valueBuffer.wrap(buffer, offset, length);
    this.valueWriter = valueBuffer;
    return this;
  }

  public SubscribedRecordWriter valueWriter(final BufferWriter valueWriter) {
    this.valueWriter = valueWriter;
    return this;
  }

  public SubscribedRecordWriter timestamp(long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  public SubscribedRecordWriter rejectionType(RejectionType rejectionType) {
    this.rejectionType = rejectionType;
    return this;
  }

  public SubscribedRecordWriter rejectionReason(DirectBuffer rejectionReason) {
    this.rejectionReason.wrap(rejectionReason, 0, rejectionReason.capacity());
    return this;
  }

  @Override
  public int getLength() {
    return MessageHeaderEncoder.ENCODED_LENGTH
        + SubscribedRecordEncoder.BLOCK_LENGTH
        + SubscribedRecordEncoder.valueHeaderLength()
        + valueWriter.getLength()
        + SubscribedRecordEncoder.rejectionReasonHeaderLength()
        + rejectionReason.capacity();
  }

  @Override
  public void write(final MutableDirectBuffer buffer, int offset) {
    headerEncoder
        .wrap(buffer, offset)
        .blockLength(bodyEncoder.sbeBlockLength())
        .templateId(bodyEncoder.sbeTemplateId())
        .schemaId(bodyEncoder.sbeSchemaId())
        .version(bodyEncoder.sbeSchemaVersion());

    offset += MessageHeaderEncoder.ENCODED_LENGTH;

    bodyEncoder
        .wrap(buffer, offset)
        .recordType(recordType)
        .partitionId(partitionId)
        .position(position)
        .sourceRecordPosition(sourceRecordPosition)
        .key(key)
        .timestamp(timestamp)
        .subscriberKey(subscriberKey)
        .subscriptionType(subscriptionType)
        .valueType(valueType)
        .intent(intent)
        .rejectionType(rejectionType);

    offset += SubscribedRecordEncoder.BLOCK_LENGTH;

    final int eventLength = valueWriter.getLength();
    buffer.putShort(offset, (short) eventLength, Protocol.ENDIANNESS);

    offset += SubscribedRecordEncoder.valueHeaderLength();
    valueWriter.write(buffer, offset);

    offset += eventLength;
    final int rejectionReasonLength = rejectionReason.capacity();
    buffer.putShort(offset, (short) rejectionReasonLength, Protocol.ENDIANNESS);

    offset += SubscribedRecordEncoder.rejectionReasonHeaderLength();
    buffer.putBytes(offset, rejectionReason, 0, rejectionReasonLength);
  }

  public boolean tryWriteMessage(int remoteStreamId) {
    Objects.requireNonNull(valueWriter);

    try {
      return output.sendMessage(remoteStreamId, this);
    } finally {
      reset();
    }
  }

  protected void reset() {
    this.partitionId = partitionIdNullValue();
    this.position = positionNullValue();
    this.sourceRecordPosition = sourceRecordPositionNullValue();
    this.key = keyNullValue();
    this.timestamp = timestampNullValue();
    this.subscriberKey = subscriberKeyNullValue();
    this.subscriptionType = SubscriptionType.NULL_VAL;
    this.valueType = ValueType.NULL_VAL;
    this.valueWriter = null;
    this.recordType = RecordType.NULL_VAL;
    this.intent = Intent.NULL_VAL;
  }
}
