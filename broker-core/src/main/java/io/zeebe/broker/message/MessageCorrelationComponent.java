package io.zeebe.broker.message;

import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.LEADER_PARTITION_GROUP_NAME;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.TOPOLOGY_MANAGER_SERVICE;
import static io.zeebe.broker.logstreams.LogStreamServiceNames.STREAM_PROCESSOR_SERVICE_FACTORY;
import static io.zeebe.broker.message.MessageCorrelationServiceNames.MESSAGE_CORRELATION_STREAM_PROCESSOR_MANAGER;
import static io.zeebe.broker.transport.TransportServiceNames.*;
import io.zeebe.broker.system.Component;
import io.zeebe.broker.system.SystemContext;
import io.zeebe.servicecontainer.ServiceContainer;

public class MessageCorrelationComponent implements Component {

    @Override
    public void init(SystemContext context) {
        final ServiceContainer serviceContainer = context.getServiceContainer();

        final MessageCorrelationStreamProcessorManager streamProcessorManager =
            new MessageCorrelationStreamProcessorManager();

        serviceContainer
            .createService(MESSAGE_CORRELATION_STREAM_PROCESSOR_MANAGER, streamProcessorManager)
            .dependency(
                serverTransport(CLIENT_API_SERVER_NAME),
                streamProcessorManager.getClientApiTransportInjector())
            .dependency(
                TOPOLOGY_MANAGER_SERVICE, streamProcessorManager.getTopologyManagerInjector())
            .dependency(
                clientTransport(MANAGEMENT_API_CLIENT_NAME),
                streamProcessorManager.getManagementApiClientInjector())
            .dependency(
                STREAM_PROCESSOR_SERVICE_FACTORY,
                streamProcessorManager.getStreamProcessorServiceFactoryInjector())
            .groupReference(
                LEADER_PARTITION_GROUP_NAME, streamProcessorManager.getPartitionsGroupReference())
            .install();
    }

}
