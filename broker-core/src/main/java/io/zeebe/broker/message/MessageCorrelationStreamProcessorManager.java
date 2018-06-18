package io.zeebe.broker.message;

import static io.zeebe.broker.logstreams.processor.StreamProcessorIds.MESSAGE_CORRELATION_PROCESSOR_ID;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.clustering.base.topology.TopologyManager;
import io.zeebe.broker.logstreams.processor.StreamProcessorServiceFactory;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.message.processor.MessageCorrelationStreamProcessor;
import io.zeebe.servicecontainer.*;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.ServerTransport;

public class MessageCorrelationStreamProcessorManager implements Service<Void> {

    private final Injector<ServerTransport> clientApiTransportInjector = new Injector<>();
    private final Injector<ClientTransport> managementApiClientInjector = new Injector<>();
    private final Injector<TopologyManager> topologyManagerInjector = new Injector<>();

    private final Injector<StreamProcessorServiceFactory> streamProcessorServiceFactoryInjector =
            new Injector<>();

    private final ServiceGroupReference<Partition> partitionsGroupReference = ServiceGroupReference
            .<Partition>create()
            .onAdd((partitionName, partition) -> startStreamProcessor(partitionName, partition))
            .build();

    private StreamProcessorServiceFactory streamProcessorServiceFactory;

    private ServerTransport transport;
    private TopologyManager topologyManager;

    public void startStreamProcessor(ServiceName<Partition> partitionServiceName,
            Partition partition) {

        final MessageCorrelationStreamProcessor streamProcessor =
                new MessageCorrelationStreamProcessor(managementApiClientInjector.getValue(),
                        topologyManager);

        final TypedStreamEnvironment env =
                new TypedStreamEnvironment(partition.getLogStream(), transport.getOutput());

        streamProcessorServiceFactory.createService(partition, partitionServiceName)
                .processor(streamProcessor.createStreamProcessor(env))
                .processorId(MESSAGE_CORRELATION_PROCESSOR_ID).processorName("message-correlation")
                .build();
    }

    @Override
    public void start(ServiceStartContext serviceContext) {
        this.transport = clientApiTransportInjector.getValue();
        this.streamProcessorServiceFactory = streamProcessorServiceFactoryInjector.getValue();
        this.topologyManager = topologyManagerInjector.getValue();
    }

    public Injector<ServerTransport> getClientApiTransportInjector() {
        return clientApiTransportInjector;
    }

    public ServiceGroupReference<Partition> getPartitionsGroupReference() {
        return partitionsGroupReference;
    }

    public Injector<StreamProcessorServiceFactory> getStreamProcessorServiceFactoryInjector() {
        return streamProcessorServiceFactoryInjector;
    }

    public Injector<TopologyManager> getTopologyManagerInjector() {
        return topologyManagerInjector;
    }

    public Injector<ClientTransport> getManagementApiClientInjector() {
        return managementApiClientInjector;
    }

    @Override
    public Void get() {
        return null;
    }
}
