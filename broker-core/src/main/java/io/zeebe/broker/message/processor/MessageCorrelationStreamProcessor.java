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
package io.zeebe.broker.message.processor;

import io.zeebe.broker.clustering.base.topology.*;
import io.zeebe.broker.logstreams.processor.*;
import io.zeebe.broker.message.processor.MessageCorrelationState.MessageInfo;
import io.zeebe.broker.message.processor.MessageCorrelationState.MessageSubscriptionInfo;
import io.zeebe.broker.message.processor.MessageCorrelationStreamProcessor.MessageSubscriptionResponseInspector;
import io.zeebe.broker.message.record.MessageRecord;
import io.zeebe.broker.message.record.MessageSubscriptionRecord;
import io.zeebe.logstreams.processor.EventLifecycleContext;
import io.zeebe.protocol.clientapi.*;
import io.zeebe.protocol.intent.MessageIntent;
import io.zeebe.protocol.intent.MessageSubscriptionIntent;
import io.zeebe.transport.*;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.buffer.BufferWriter;
import io.zeebe.util.sched.future.ActorFuture;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;

public class MessageCorrelationStreamProcessor
    implements StreamProcessorLifecycleAware, TopologyPartitionListener {

  private ClientTransport clientTransport;
  private TopologyManager topologyManager;

  private JsonSnapshotSupport<MessageCorrelationState> state;

  private volatile Int2ObjectHashMap<RemoteAddress> partitionLeaders = new Int2ObjectHashMap<>();

  public MessageCorrelationStreamProcessor(
      ClientTransport clientTransport, TopologyManager topologyManager) {
    this.clientTransport = clientTransport;
    this.topologyManager = topologyManager;
  }

  public TypedStreamProcessor createStreamProcessor(TypedStreamEnvironment environment) {

    state = new JsonSnapshotSupport<>(MessageCorrelationState.class);

    return environment
        .newStreamProcessor()
        .onCommand(ValueType.MESSAGE, MessageIntent.PUBLISH, new PublishMessageEventHandler())
        .onCommand(
            ValueType.MESSAGE_SUBSCRIPTION,
            MessageSubscriptionIntent.SUBSCRIBE,
            new SubscribeCommandHandler())
        .onEvent(
            ValueType.MESSAGE_SUBSCRIPTION,
            MessageSubscriptionIntent.CORRELATED,
            new CorrelatedHandler())
        .withStateResource(state)
        .build();
  }

  @Override
  public void onOpen(TypedStreamProcessor streamProcessor) {
    topologyManager.addTopologyPartitionListener(this);
  }

  @Override
  public void onClose() {
    topologyManager.removeTopologyPartitionListener(this);
  }

  public class PublishMessageEventHandler implements TypedRecordProcessor<MessageRecord> {

    private MessageSubscriptionInfo correlatedSubscription;
    private final MessageSubscriptionRecord correlatedSubscriptionRecord =
        new MessageSubscriptionRecord();

    @Override
    public void processRecord(TypedRecord<MessageRecord> record) {

      correlatedSubscription = null;

      final MessageCorrelationState data = state.getData();

      final MessageRecord value = record.getValue();

      final String name = BufferUtil.bufferAsString(value.getMessageName());
      final String key = BufferUtil.bufferAsString(value.getMessageKey());

      correlatedSubscription = data.getNextSubscription(name, key);
    }

    @Override
    public boolean executeSideEffects(
        TypedRecord<MessageRecord> record, TypedResponseWriter responseWriter) {
      return responseWriter.writeEvent(MessageIntent.PUBLISHED, record);
    }

    @Override
    public long writeRecord(TypedRecord<MessageRecord> record, TypedStreamWriter writer) {

      if (correlatedSubscription != null) {

        correlatedSubscriptionRecord.reset();

        correlatedSubscriptionRecord
            .setMessageName(BufferUtil.wrapString(correlatedSubscription.messageName))
            .setMessageKey(BufferUtil.wrapString(correlatedSubscription.messageKey))
            .setActivityInstaneId(correlatedSubscription.activityInstanceId)
            .setWorkflowInstanceKey(correlatedSubscription.workflowInstanceKey)
            .setPartitionId(correlatedSubscription.partitionId);

        return writer
            .newBatch()
            .addFollowUpEvent(record.getKey(), MessageIntent.PUBLISHED, record.getValue())
            .addFollowUpEvent(
                 correlatedSubscription.subscriptionKey, MessageSubscriptionIntent.CORRELATED, correlatedSubscriptionRecord)
            .write();
      } else {
        return writer.writeFollowUpEvent(
            record.getKey(), MessageIntent.PUBLISHED, record.getValue());
      }
    }

    @Override
    public void updateState(TypedRecord<MessageRecord> record) {

      if (correlatedSubscription == null) {
        final MessageRecord value = record.getValue();

        final String name = BufferUtil.bufferAsString(value.getMessageName());
        final String key = BufferUtil.bufferAsString(value.getMessageKey());

        final MessageInfo messageInfo = new MessageInfo();
        messageInfo.messageName = name;
        messageInfo.messageKey = key;
        messageInfo.payload = BufferUtil.cloneBuffer(value.getPayload()).byteArray();

        state.getData().addMessage(messageInfo);
      }
    }
  }

  public class SubscribeCommandHandler implements TypedRecordProcessor<MessageSubscriptionRecord> {

    private MessageInfo correlatedMessage;
    private final MessageSubscriptionRecord correlatedSubscriptionRecord =
        new MessageSubscriptionRecord();

    @Override
    public void processRecord(TypedRecord<MessageSubscriptionRecord> record) {

      correlatedMessage = null;

      final MessageCorrelationState data = state.getData();

      final MessageSubscriptionRecord value = record.getValue();

      final String name = BufferUtil.bufferAsString(value.getMessageName());
      final String key = BufferUtil.bufferAsString(value.getMessageKey());

      correlatedMessage = data.getNextMessage(name, key);
    }

    @Override
    public boolean executeSideEffects(
        TypedRecord<MessageSubscriptionRecord> record, TypedResponseWriter responseWriter) {

      return responseWriter.writeEvent(MessageSubscriptionIntent.SUBSCRIBED, record);
    }

    @Override
    public long writeRecord(
        TypedRecord<MessageSubscriptionRecord> record, TypedStreamWriter writer) {
      if (correlatedMessage != null) {
        correlatedSubscriptionRecord.reset();

        final MessageSubscriptionRecord value = record.getValue();

        correlatedSubscriptionRecord
            .setMessageName(BufferUtil.wrapString(correlatedMessage.messageName))
            .setMessageKey(BufferUtil.wrapString(correlatedMessage.messageKey))
            .setActivityInstaneId(value.getActivityInstanceId())
            .setWorkflowInstanceKey(value.getWorkflowInstanceKey())
            .setPartitionId(value.getParitionId());

        return writer
            .newBatch()
            .addFollowUpEvent(
                record.getKey(), MessageSubscriptionIntent.SUBSCRIBED, record.getValue())
            .addFollowUpEvent(
                record.getKey(), MessageSubscriptionIntent.CORRELATED, correlatedSubscriptionRecord)
            .write();
      } else {
        return writer.writeFollowUpEvent(
            record.getKey(), MessageSubscriptionIntent.SUBSCRIBED, record.getValue());
      }
    }

    @Override
    public void updateState(TypedRecord<MessageSubscriptionRecord> record) {

      if (correlatedMessage == null) {
        final MessageSubscriptionRecord value = record.getValue();

        final String name = BufferUtil.bufferAsString(value.getMessageName());
        final String key = BufferUtil.bufferAsString(value.getMessageKey());

        final MessageSubscriptionInfo subscriptionInfo = new MessageSubscriptionInfo();
        subscriptionInfo.messageName = name;
        subscriptionInfo.messageKey = key;
        subscriptionInfo.activityInstanceId = value.getActivityInstanceId();
        subscriptionInfo.workflowInstanceKey = value.getWorkflowInstanceKey();
        subscriptionInfo.partitionId = value.getParitionId();
        subscriptionInfo.subscriptionKey = record.getKey();

        state.getData().addSubscription(subscriptionInfo);
      }
    }
  }

  public class CorrelatedHandler implements TypedRecordProcessor<MessageSubscriptionRecord> {

      @Override
    public void processRecord(TypedRecord<MessageSubscriptionRecord> record) {
        System.out.println("foo");
    }

    @Override
    public void processRecord(
        TypedRecord<MessageSubscriptionRecord> record, EventLifecycleContext ctx) {

      final MessageSubscriptionRecord value = record.getValue();
      final int paritionId = value.getParitionId();

      final ClientOutput output = clientTransport.getOutput();

      final BufferWriter requestWriter = new MessageSubscriptionRequest(value, paritionId);
      final Supplier<RemoteAddress> remoteAddressSupplier = () -> partitionLeaders.get(paritionId);
      final Predicate<DirectBuffer> responseInspector = new MessageSubscriptionResponseInspector();
      final ActorFuture<ClientResponse> future =
          output.sendRequestWithRetry(
              remoteAddressSupplier, responseInspector, requestWriter, Duration.ofSeconds(30));

      ctx.async(future);
    }
  }

  @Override
  public void onPartitionUpdated(PartitionInfo partitionInfo, NodeInfo member) {

    topologyManager.query(
        (t) -> {
          final Collection<PartitionInfo> partitions = t.getPartitions();
          final Int2ObjectHashMap<RemoteAddress> leaders = new Int2ObjectHashMap<>();

          partitions
              .stream()
              .forEach(
                  p -> {
                    final NodeInfo nodeInfo = t.getLeader(p.getPartitionId());
                    if (nodeInfo != null) {
                      leaders.put(
                          p.getPartitionId(),
                          clientTransport.getRemoteAddress(nodeInfo.getClientApiAddress()));
                    }
                  });

          partitionLeaders = leaders;

          return null;
        });
  }

  class MessageSubscriptionRequest implements BufferWriter {
    private final MessageSubscriptionRecord record;
    private final int partitionId;

    private MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private ExecuteCommandRequestEncoder encoder = new ExecuteCommandRequestEncoder();

    MessageSubscriptionRequest(MessageSubscriptionRecord record, int partitionId) {
      this.record = record;
      this.partitionId = partitionId;
    }

    @Override
    public void write(MutableDirectBuffer buffer, int offset) {
      headerEncoder
          .wrap(buffer, offset)
          .blockLength(encoder.sbeBlockLength())
          .schemaId(encoder.sbeSchemaId())
          .templateId(encoder.sbeTemplateId())
          .version(encoder.sbeSchemaVersion());

      offset += headerEncoder.encodedLength();

      encoder.wrap(buffer, offset);

      encoder
          .partitionId(partitionId)
          .sourceRecordPosition(ExecuteCommandRequestEncoder.sourceRecordPositionNullValue())
          .position(ExecuteCommandRequestEncoder.positionNullValue())
          .key(ExecuteCommandRequestEncoder.keyNullValue());

      encoder.valueType(ValueType.MESSAGE_SUBSCRIPTION);
      encoder.intent(MessageSubscriptionIntent.CORRELATED.getIntent());

      offset = encoder.limit();

      buffer.putShort(offset, (short) record.getLength(), ByteOrder.LITTLE_ENDIAN);

      offset += ExecuteCommandRequestEncoder.valueHeaderLength();

      record.write(encoder.buffer(), offset);
    }

    @Override
    public int getLength() {
      return MessageHeaderEncoder.ENCODED_LENGTH
          + ExecuteCommandRequestEncoder.intentEncodingLength()
          + ExecuteCommandRequestEncoder.keyEncodingLength()
          + ExecuteCommandRequestEncoder.partitionIdEncodingLength()
          + ExecuteCommandRequestEncoder.positionEncodingLength()
          + ExecuteCommandRequestEncoder.sourceRecordPositionEncodingLength()
          + ExecuteCommandRequestEncoder.valueTypeEncodingLength()
          + ExecuteCommandRequestEncoder.valueHeaderLength()
          + record.getLength();
    }
  }

  public class MessageSubscriptionResponseInspector implements Predicate<DirectBuffer> {

    private MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private ExecuteCommandResponseDecoder decoder = new ExecuteCommandResponseDecoder();

    @Override
    public boolean test(DirectBuffer buffer) {

      headerDecoder.wrap(buffer, 0);

      if (headerDecoder.schemaId() != decoder.sbeSchemaId()
          || headerDecoder.templateId() != decoder.sbeTemplateId()) {
        return true;
      } else {
        decoder.wrap(
            buffer,
            headerDecoder.encodedLength(),
            headerDecoder.blockLength(),
            headerDecoder.version());

        return decoder.intent() != MessageSubscriptionIntent.SUBSCRIBED.getIntent();
      }
    }
  }
}
