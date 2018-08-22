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
package io.zeebe.broker.system.workflow.repository.service;

import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.clustering.base.topology.TopologyManager;
import io.zeebe.broker.logstreams.processor.KeyGenerator;
import io.zeebe.broker.logstreams.processor.StreamProcessorIds;
import io.zeebe.broker.logstreams.processor.StreamProcessorLifecycleAware;
import io.zeebe.broker.logstreams.processor.StreamProcessorServiceFactory;
import io.zeebe.broker.logstreams.processor.StreamProcessorServiceFactory.Builder;
import io.zeebe.broker.logstreams.processor.TypedEventStreamProcessorBuilder;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.logstreams.processor.TypedStreamProcessor;
import io.zeebe.broker.system.SystemServiceNames;
import io.zeebe.broker.system.management.LeaderManagementRequestHandler;
import io.zeebe.broker.system.workflow.repository.api.client.GetWorkflowControlMessageHandler;
import io.zeebe.broker.system.workflow.repository.api.client.ListWorkflowsControlMessageHandler;
import io.zeebe.broker.system.workflow.repository.api.management.FetchWorkflowRequestHandler;
import io.zeebe.broker.system.workflow.repository.processor.DeploymentCreateEventProcessor;
import io.zeebe.broker.system.workflow.repository.processor.DeploymentCreatedEventProcessor;
import io.zeebe.broker.system.workflow.repository.processor.DeploymentRejectedEventProcessor;
import io.zeebe.broker.system.workflow.repository.processor.state.PendingDeploymentsStateController;
import io.zeebe.broker.system.workflow.repository.processor.state.WorkflowRepositoryIndex;
import io.zeebe.broker.transport.controlmessage.ControlMessageHandlerManager;
import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.log.LogStreamWriterImpl;
import io.zeebe.logstreams.processor.StreamProcessorContext;
import io.zeebe.logstreams.state.StateSnapshotController;
import io.zeebe.logstreams.state.StateStorage;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.DeploymentIntent;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceGroupReference;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.ServerTransport;

public class DeploymentManager implements Service<DeploymentManager> {
  private final ServiceGroupReference<Partition> partitionsGroupReference =
      ServiceGroupReference.<Partition>create()
          .onAdd((name, partition) -> installServices(partition, name))
          .build();

  private final Injector<StreamProcessorServiceFactory> streamProcessorServiceFactoryInjector =
      new Injector<>();
  private final Injector<ServerTransport> clientApiTransportInjector = new Injector<>();
  private final Injector<LeaderManagementRequestHandler> requestHandlerServiceInjector =
      new Injector<>();
  private final Injector<ControlMessageHandlerManager> controlMessageHandlerManagerServiceInjector =
      new Injector<>();

  private final Injector<TopologyManager> topologyManagerInjector = new Injector<>();
  private final Injector<ClientTransport> managementApiClientInjector = new Injector<>();

  private ServerTransport clientApiTransport;
  private StreamProcessorServiceFactory streamProcessorServiceFactory;

  private LeaderManagementRequestHandler requestHandlerService;

  private ServiceStartContext startContext;

  private GetWorkflowControlMessageHandler getWorkflowMessageHandler;
  private ListWorkflowsControlMessageHandler listWorkflowsControlMessageHandler;

  private TopologyManager topologyManager;
  private ClientTransport managementApi;

  @Override
  public void start(ServiceStartContext startContext) {
    this.startContext = startContext;
    this.clientApiTransport = clientApiTransportInjector.getValue();
    this.streamProcessorServiceFactory = streamProcessorServiceFactoryInjector.getValue();
    this.requestHandlerService = requestHandlerServiceInjector.getValue();
    this.topologyManager = topologyManagerInjector.getValue();
    this.managementApi = managementApiClientInjector.getValue();

    getWorkflowMessageHandler =
        new GetWorkflowControlMessageHandler(clientApiTransport.getOutput());
    listWorkflowsControlMessageHandler =
        new ListWorkflowsControlMessageHandler(clientApiTransport.getOutput());

    final ControlMessageHandlerManager controlMessageHandlerManager =
        controlMessageHandlerManagerServiceInjector.getValue();
    controlMessageHandlerManager.registerHandler(getWorkflowMessageHandler);
    controlMessageHandlerManager.registerHandler(listWorkflowsControlMessageHandler);
  }

  private void installServices(
      final Partition partition, ServiceName<Partition> partitionServiceName) {

    if (partition.getInfo().getPartitionId() == Protocol.SYSTEM_PARTITION) {
      return;
    }

    final String processorName = "deployment-" + partition.getInfo().getPartitionId();
    final int deploymentProcessorId = StreamProcessorIds.DEPLOYMENT_PROCESSOR_ID;

    final Builder streamProcessorServiceBuilder =
        streamProcessorServiceFactory
            .createService(partition, partitionServiceName)
            .processorId(deploymentProcessorId)
            .processorName(processorName);

    final TypedStreamEnvironment streamEnvironment =
        new TypedStreamEnvironment(partition.getLogStream(), clientApiTransport.getOutput());

    final WorkflowRepositoryIndex repositoryIndex = new WorkflowRepositoryIndex();

    final TypedEventStreamProcessorBuilder streamProcessorBuilder =
        streamEnvironment
            .newStreamProcessor()
            .keyGenerator(KeyGenerator.createDeploymentKeyGenerator())
            .onEvent(
                ValueType.DEPLOYMENT,
                DeploymentIntent.CREATED,
                new DeploymentCreatedEventProcessor(repositoryIndex))
            .withStateResource(repositoryIndex);

    if (partition.getInfo().getPartitionId() == Protocol.DEPLOYMENT_PARTITION) {

      final PendingDeploymentsStateController pendingDeploymentsStateController =
          new PendingDeploymentsStateController();
      final LogStreamWriterImpl logStreamWriter = new LogStreamWriterImpl(partition.getLogStream());

      final DeploymentCreateEventProcessor createEventProcessor =
          new DeploymentCreateEventProcessor(
              topologyManager,
              repositoryIndex,
              pendingDeploymentsStateController,
              managementApi,
              logStreamWriter);

      streamProcessorBuilder
          .onCommand(ValueType.DEPLOYMENT, DeploymentIntent.CREATE, createEventProcessor)
          .onRejection(
              ValueType.DEPLOYMENT, DeploymentIntent.CREATE, new DeploymentRejectedEventProcessor())
          .withStateController(pendingDeploymentsStateController);

      addListnerToInstallFetchWorkflowService(
          partitionServiceName, repositoryIndex, streamProcessorBuilder);

      final StateStorage stateStorage =
          partition.getStateStorageFactory().create(deploymentProcessorId, processorName);

      final StateSnapshotController stateSnapshotController =
          new StateSnapshotController(pendingDeploymentsStateController, stateStorage);

      streamProcessorServiceBuilder.snapshotController(stateSnapshotController);
    }

    streamProcessorServiceBuilder.processor(streamProcessorBuilder.build()).build();
  }

  private void addListnerToInstallFetchWorkflowService(
      ServiceName<Partition> partitionServiceName,
      WorkflowRepositoryIndex repositoryIndex,
      TypedEventStreamProcessorBuilder streamProcessorBuilder) {

    streamProcessorBuilder.withListener(
        new StreamProcessorLifecycleAware() {
          private BufferedLogStreamReader reader;

          // Only expose the fetch workflow and workflow repository APIs after reprocessing
          // to avoid that we
          // cannot (yet) return a workflow that we were previously able to return
          @Override
          public void onRecovered(TypedStreamProcessor streamProcessor) {
            final StreamProcessorContext ctx = streamProcessor.getStreamProcessorContext();

            reader = new BufferedLogStreamReader();
            reader.wrap(ctx.getLogStream());

            final DeploymentResourceCache cache = new DeploymentResourceCache(reader);

            final WorkflowRepositoryService workflowRepositoryService =
                new WorkflowRepositoryService(ctx.getActorControl(), repositoryIndex, cache);

            startContext
                .createService(SystemServiceNames.REPOSITORY_SERVICE, workflowRepositoryService)
                .dependency(partitionServiceName)
                .install();

            final FetchWorkflowRequestHandler requestHandler =
                new FetchWorkflowRequestHandler(workflowRepositoryService);
            requestHandlerService.setFetchWorkflowRequestHandler(requestHandler);

            getWorkflowMessageHandler.setWorkflowRepositoryService(workflowRepositoryService);
            listWorkflowsControlMessageHandler.setWorkflowRepositoryService(
                workflowRepositoryService);
          }

          @Override
          public void onClose() {
            requestHandlerService.setFetchWorkflowRequestHandler(null);
            getWorkflowMessageHandler.setWorkflowRepositoryService(null);
            listWorkflowsControlMessageHandler.setWorkflowRepositoryService(null);

            reader.close();
          }
        });
  }

  @Override
  public DeploymentManager get() {
    return this;
  }

  public ServiceGroupReference<Partition> getPartitionsGroupReference() {
    return partitionsGroupReference;
  }

  public Injector<StreamProcessorServiceFactory> getStreamProcessorServiceFactoryInjector() {
    return streamProcessorServiceFactoryInjector;
  }

  public Injector<ServerTransport> getClientApiTransportInjector() {
    return clientApiTransportInjector;
  }

  public Injector<LeaderManagementRequestHandler> getRequestHandlerServiceInjector() {
    return requestHandlerServiceInjector;
  }

  public Injector<ControlMessageHandlerManager> getControlMessageHandlerManagerServiceInjector() {
    return controlMessageHandlerManagerServiceInjector;
  }

  public Injector<TopologyManager> getTopologyManagerInjector() {
    return topologyManagerInjector;
  }

  public Injector<ClientTransport> getManagementApiClientInjector() {
    return managementApiClientInjector;
  }
}
