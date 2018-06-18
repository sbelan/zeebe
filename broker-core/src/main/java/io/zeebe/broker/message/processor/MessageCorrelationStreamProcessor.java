package io.zeebe.broker.message.processor;

import io.zeebe.broker.clustering.base.topology.TopologyManager;
import io.zeebe.broker.logstreams.processor.*;
import io.zeebe.broker.message.record.MessageRecord;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.MessageIntent;
import io.zeebe.transport.ClientTransport;

public class MessageCorrelationStreamProcessor implements StreamProcessorLifecycleAware {

    private ClientTransport clientTransport;
    private TopologyManager topologyManager;

    public MessageCorrelationStreamProcessor(ClientTransport clientTransport,
            TopologyManager topologyManager) {
        this.clientTransport = clientTransport;
        this.topologyManager = topologyManager;
    }

    public TypedStreamProcessor createStreamProcessor(TypedStreamEnvironment environment) {

        return environment.newStreamProcessor()
           .onCommand(ValueType.MESSAGE, MessageIntent.PUBLISH,
                new PublishMessageEventHandler()).build();
    }

    public class PublishMessageEventHandler implements TypedRecordProcessor<MessageRecord> {

        @Override
        public boolean executeSideEffects(TypedRecord<MessageRecord> record,
                TypedResponseWriter responseWriter) {
            return responseWriter.writeEvent(MessageIntent.PUBLISHED, record);
        }

        @Override
        public long writeRecord(TypedRecord<MessageRecord> record, TypedStreamWriter writer) {
            return writer.writeFollowUpEvent(record.getKey(), MessageIntent.PUBLISHED, record.getValue());
        }
    }

}
