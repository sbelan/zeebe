package io.zeebe.broker.message.processor;

import io.zeebe.broker.clustering.base.topology.TopologyManager;
import io.zeebe.broker.logstreams.processor.*;
import io.zeebe.broker.message.processor.MessageCorrelationState.MessageInfo;
import io.zeebe.broker.message.processor.MessageCorrelationState.MessageSubscriptionInfo;
import io.zeebe.broker.message.record.MessageRecord;
import io.zeebe.broker.message.record.MessageSubscriptionRecord;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.MessageIntent;
import io.zeebe.protocol.intent.MessageSubscriptionIntent;
import io.zeebe.transport.ClientTransport;
import io.zeebe.util.buffer.BufferUtil;

public class MessageCorrelationStreamProcessor implements StreamProcessorLifecycleAware {

    private ClientTransport clientTransport;
    private TopologyManager topologyManager;

    private JsonSnapshotSupport<MessageCorrelationState> state;

    public MessageCorrelationStreamProcessor(ClientTransport clientTransport,
            TopologyManager topologyManager) {
        this.clientTransport = clientTransport;
        this.topologyManager = topologyManager;
    }

    public TypedStreamProcessor createStreamProcessor(TypedStreamEnvironment environment) {

        state = new JsonSnapshotSupport<>(MessageCorrelationState.class);

        return environment.newStreamProcessor()
           .onCommand(ValueType.MESSAGE, MessageIntent.PUBLISH,
                new PublishMessageEventHandler())
           .onCommand(ValueType.MESSAGE_SUBSCRIPTION, MessageSubscriptionIntent.SUBSCRIBE,
                new SubscribeCommandHandler())
           .onEvent(ValueType.MESSAGE_SUBSCRIPTION, MessageSubscriptionIntent.CORRELATED,
                   new CorrelatedHandler())
           .withStateResource(state)
           .build();
    }

    public class PublishMessageEventHandler implements TypedRecordProcessor<MessageRecord> {

        private MessageSubscriptionInfo correlatedSubscription;
        private final MessageSubscriptionRecord correlatedSubscriptionRecord = new MessageSubscriptionRecord();

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
        public boolean executeSideEffects(TypedRecord<MessageRecord> record,
                TypedResponseWriter responseWriter) {
            return responseWriter.writeEvent(MessageIntent.PUBLISHED, record);
        }

        @Override
        public long writeRecord(TypedRecord<MessageRecord> record, TypedStreamWriter writer) {

            if (correlatedSubscription != null)
            {
                correlatedSubscriptionRecord.reset();

                correlatedSubscriptionRecord.setMessageName(BufferUtil.wrapString(correlatedSubscription.messageName))
                    .setMessageKey(BufferUtil.wrapString(correlatedSubscription.messageKey))
                    .setActivityInstaneId(correlatedSubscription.activityInstanceId)
                    .setWorkflowInstanceKey(correlatedSubscription.workflowInstanceKey)
                    .setPartitionId(correlatedSubscription.partitionId);

                return writer.newBatch()
                    .addFollowUpEvent(record.getKey(), MessageIntent.PUBLISHED, record.getValue())
                    .addFollowUpEvent(record.getKey(), MessageSubscriptionIntent.CORRELATED, correlatedSubscriptionRecord)
                    .write();
            }
            else
            {
                return writer.writeFollowUpEvent(record.getKey(), MessageIntent.PUBLISHED, record.getValue());
            }
        }

        @Override
        public void updateState(TypedRecord<MessageRecord> record) {

            if (correlatedSubscription == null)
            {
                final MessageRecord value = record.getValue();

                final String name = BufferUtil.bufferAsString(value.getMessageName());
                final String key = BufferUtil.bufferAsString(value.getMessageKey());

                final MessageInfo messageInfo = new MessageInfo();
                messageInfo.messageName = name;
                messageInfo.messageKey = key;
                messageInfo.payload = BufferUtil.cloneBuffer(value.getPayload()).byteArray();

                state.getData()
                    .addMessage(messageInfo);
            }
        }
    }

    public class SubscribeCommandHandler implements TypedRecordProcessor<MessageSubscriptionRecord> {

        private MessageInfo correlatedMessage;
        private final MessageSubscriptionRecord correlatedSubscriptionRecord = new MessageSubscriptionRecord();

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
        public boolean executeSideEffects(TypedRecord<MessageSubscriptionRecord> record,
                TypedResponseWriter responseWriter) {

            return responseWriter.writeEvent(MessageSubscriptionIntent.SUBSCRIBED, record);
        }

        @Override
        public long writeRecord(TypedRecord<MessageSubscriptionRecord> record, TypedStreamWriter writer) {
            if (correlatedMessage != null)
            {
                correlatedSubscriptionRecord.reset();

                final MessageSubscriptionRecord value = record.getValue();

                correlatedSubscriptionRecord.setMessageName(BufferUtil.wrapString(correlatedMessage.messageName))
                    .setMessageKey(BufferUtil.wrapString(correlatedMessage.messageKey))
                    .setActivityInstaneId(value.getActivityInstanceId())
                    .setWorkflowInstanceKey(value.getWorkflowInstanceKey())
                    .setPartitionId(value.getParitionId());

                return writer.newBatch()
                    .addFollowUpEvent(record.getKey(), MessageSubscriptionIntent.SUBSCRIBED, record.getValue())
                    .addFollowUpEvent(record.getKey(), MessageSubscriptionIntent.CORRELATED, correlatedSubscriptionRecord)
                    .write();
            }
            else
            {
                return writer.writeFollowUpEvent(record.getKey(), MessageSubscriptionIntent.SUBSCRIBED, record.getValue());
            }
        }

        @Override
        public void updateState(TypedRecord<MessageSubscriptionRecord> record) {

            if (correlatedMessage == null)
            {
                final MessageSubscriptionRecord value = record.getValue();

                final String name = BufferUtil.bufferAsString(value.getMessageName());
                final String key = BufferUtil.bufferAsString(value.getMessageKey());

                final MessageSubscriptionInfo subscriptionInfo = new MessageSubscriptionInfo();
                subscriptionInfo.messageName = name;
                subscriptionInfo.messageKey = key;
                subscriptionInfo.activityInstanceId = value.getActivityInstanceId();
                subscriptionInfo.workflowInstanceKey = value.getWorkflowInstanceKey();
                subscriptionInfo.partitionId = value.getParitionId();

                state.getData()
                    .addSubscription(subscriptionInfo);
            }
        }
    }

    public class CorrelatedHandler implements TypedRecordProcessor<MessageSubscriptionRecord> {

    }
}
