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
package io.zeebe.broker.clustering;

import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.CLUSTERING_BASE_LAYER;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.FOLLOWER_PARTITION_GROUP_NAME;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.GOSSIP_JOIN_SERVICE;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.GOSSIP_SERVICE;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.LEADER_PARTITION_GROUP_NAME;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.LEADER_PARTITION_SYSTEM_GROUP_NAME;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.MANAGEMENT_API_REQUEST_HANDLER_SERVICE_NAME;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.RAFT_BOOTSTRAP_SERVICE;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.RAFT_CONFIGURATION_MANAGER;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.RAFT_SERVICE_GROUP;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.REMOTE_ADDRESS_MANAGER_SERVICE;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.SNAPSHOT_REPLICATION_INSTALL_SERVICE_NAME;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.SYSTEM_PARTITION_BOOTSTRAP_EXPECTED_SERVICE_NAME;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.SYSTEM_PARTITION_BOOTSTRAP_SERVICE_NAME;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.TOPOLOGY_MANAGER_SERVICE;
import static io.zeebe.broker.clustering.orchestration.ClusterOrchestrationLayerServiceNames.CLUSTER_ORCHESTRATION_INSTALL_SERVICE_NAME;
import static io.zeebe.broker.transport.TransportServiceNames.CLIENT_API_SERVER_NAME;
import static io.zeebe.broker.transport.TransportServiceNames.CONTROL_MESSAGE_HANDLER_MANAGER;
import static io.zeebe.broker.transport.TransportServiceNames.MANAGEMENT_API_CLIENT_NAME;
import static io.zeebe.broker.transport.TransportServiceNames.MANAGEMENT_API_SERVER_NAME;
import static io.zeebe.broker.transport.TransportServiceNames.REPLICATION_API_CLIENT_NAME;
import static io.zeebe.broker.transport.TransportServiceNames.bufferingServerTransport;
import static io.zeebe.broker.transport.TransportServiceNames.clientTransport;
import static io.zeebe.broker.transport.TransportServiceNames.serverTransport;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.api.ManagementApiRequestHandlerService;
import io.zeebe.broker.clustering.base.bootstrap.BootstrapExpectNodes;
import io.zeebe.broker.clustering.base.bootstrap.BootstrapLocalPartitions;
import io.zeebe.broker.clustering.base.bootstrap.BootstrapSystemTopic;
import io.zeebe.broker.clustering.base.connections.RemoteAddressManager;
import io.zeebe.broker.clustering.base.gossip.GossipJoinService;
import io.zeebe.broker.clustering.base.gossip.GossipService;
import io.zeebe.broker.clustering.base.raft.RaftPersistentConfigurationManagerService;
import io.zeebe.broker.clustering.base.snapshots.SnapshotReplicationInstallService;
import io.zeebe.broker.clustering.base.topology.TopologyManagerService;
import io.zeebe.broker.clustering.orchestration.ClusterOrchestrationInstallService;
import io.zeebe.broker.system.Component;
import io.zeebe.broker.system.SystemContext;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.broker.transport.TransportServiceNames;
import io.zeebe.servicecontainer.CompositeServiceBuilder;
import io.zeebe.servicecontainer.ServiceContainer;
import org.slf4j.Logger;

/** Installs the clustering component into the broker. */
public class ClusterComponent implements Component {
  private static final Logger LOG = Loggers.CLUSTERING_LOGGER;

  @Override
  public void init(final SystemContext context) {
    final ServiceContainer serviceContainer = context.getServiceContainer();

    initClusterBaseLayer(context, serviceContainer);
    initBootstrapSystemPartition(context, serviceContainer);
    initClusterOrchestrationLayer(serviceContainer);
  }

  private void initClusterBaseLayer(
      final SystemContext context, final ServiceContainer serviceContainer) {
    final BrokerCfg brokerConfig = context.getBrokerConfiguration();
    final CompositeServiceBuilder baseLayerInstall =
        serviceContainer.createComposite(CLUSTERING_BASE_LAYER);

    final TopologyManagerService topologyManagerService =
        new TopologyManagerService(brokerConfig.getNetwork());
    baseLayerInstall
        .createService(TOPOLOGY_MANAGER_SERVICE, topologyManagerService)
        .dependency(GOSSIP_SERVICE, topologyManagerService.getGossipInjector())
        .groupReference(RAFT_SERVICE_GROUP, topologyManagerService.getRaftReference())
        .install();

    final RemoteAddressManager remoteAddressManager = new RemoteAddressManager();
    baseLayerInstall
        .createService(REMOTE_ADDRESS_MANAGER_SERVICE, remoteAddressManager)
        .dependency(TOPOLOGY_MANAGER_SERVICE, remoteAddressManager.getTopologyManagerInjector())
        .dependency(
            clientTransport(MANAGEMENT_API_CLIENT_NAME),
            remoteAddressManager.getManagementClientTransportInjector())
        .dependency(
            clientTransport(REPLICATION_API_CLIENT_NAME),
            remoteAddressManager.getReplicationClientTransportInjector())
        .install();

    final ManagementApiRequestHandlerService managementApiRequestHandlerService =
        new ManagementApiRequestHandlerService(brokerConfig);
    baseLayerInstall
        .createService(
            MANAGEMENT_API_REQUEST_HANDLER_SERVICE_NAME, managementApiRequestHandlerService)
        .dependency(
            bufferingServerTransport(MANAGEMENT_API_SERVER_NAME),
            managementApiRequestHandlerService.getServerTransportInjector())
        .dependency(
            RAFT_CONFIGURATION_MANAGER,
            managementApiRequestHandlerService.getRaftPersistentConfigurationManagerInjector())
        .groupReference(
            LEADER_PARTITION_GROUP_NAME,
            managementApiRequestHandlerService.getLeaderPartitionsGroupReference())
        .groupReference(
            LEADER_PARTITION_SYSTEM_GROUP_NAME,
            managementApiRequestHandlerService.getLeaderPartitionsGroupReference())
        .install();

    final SnapshotReplicationInstallService snapshotReplicationInstallService =
        new SnapshotReplicationInstallService(brokerConfig);
    baseLayerInstall
        .createService(SNAPSHOT_REPLICATION_INSTALL_SERVICE_NAME, snapshotReplicationInstallService)
        .groupReference(
            FOLLOWER_PARTITION_GROUP_NAME,
            snapshotReplicationInstallService.getFollowerPartitionsGroupReference())
        .install();

    initGossip(baseLayerInstall, context);
    initRaft(baseLayerInstall, context);

    context.addRequiredStartAction(baseLayerInstall.install());
  }

  private void initGossip(
      final CompositeServiceBuilder baseLayerInstall, final SystemContext context) {
    final GossipService gossipService = new GossipService(context.getBrokerConfiguration());
    baseLayerInstall
        .createService(GOSSIP_SERVICE, gossipService)
        .dependency(
            TransportServiceNames.clientTransport(TransportServiceNames.MANAGEMENT_API_CLIENT_NAME),
            gossipService.getClientTransportInjector())
        .dependency(
            TransportServiceNames.bufferingServerTransport(
                TransportServiceNames.MANAGEMENT_API_SERVER_NAME),
            gossipService.getBufferingServerTransportInjector())
        .install();

    // TODO: decide whether failure to join gossip cluster should result in broker startup fail
    final GossipJoinService gossipJoinService =
        new GossipJoinService(context.getBrokerConfiguration().getCluster());
    baseLayerInstall
        .createService(GOSSIP_JOIN_SERVICE, gossipJoinService)
        .dependency(GOSSIP_SERVICE, gossipJoinService.getGossipInjector())
        .install();
  }

  private void initRaft(
      final CompositeServiceBuilder baseLayerInstall, final SystemContext context) {
    final RaftPersistentConfigurationManagerService raftConfigurationManagerService =
        new RaftPersistentConfigurationManagerService(context.getBrokerConfiguration());
    baseLayerInstall
        .createService(RAFT_CONFIGURATION_MANAGER, raftConfigurationManagerService)
        .install();

    final BootstrapLocalPartitions raftBootstrapService =
        new BootstrapLocalPartitions(context.getBrokerConfiguration());
    baseLayerInstall
        .createService(RAFT_BOOTSTRAP_SERVICE, raftBootstrapService)
        .dependency(
            RAFT_CONFIGURATION_MANAGER, raftBootstrapService.getConfigurationManagerInjector())
        .install();
  }

  private void initBootstrapSystemPartition(
      final SystemContext context, final ServiceContainer serviceContainer) {
    final BrokerCfg brokerConfiguration = context.getBrokerConfiguration();

    final int bootstrap = brokerConfiguration.getBootstrap();

    if (bootstrap == 1) {
      LOG.info("Starting standalone broker. Configuration: {}", brokerConfiguration);

      final BootstrapSystemTopic systemPartitionBootstrapService =
          new BootstrapSystemTopic(1, context.getBrokerConfiguration());

      serviceContainer
          .createService(SYSTEM_PARTITION_BOOTSTRAP_SERVICE_NAME, systemPartitionBootstrapService)
          .dependency(RAFT_BOOTSTRAP_SERVICE)
          .dependency(
              RAFT_CONFIGURATION_MANAGER,
              systemPartitionBootstrapService.getRaftPersistentConfigurationManagerInjector())
          .install();
    } else {
      LOG.info("Starting clustered broker. Configuration: {}", brokerConfiguration);

      if (bootstrap > 0) {
        LOG.info(
            "Node started in bootstrap mode. Expecting {} nodes to join the cluster before bootstrap.",
            bootstrap);

        final int replicationFactor = bootstrap;

        final BootstrapExpectNodes bootstrapExpectService =
            new BootstrapExpectNodes(
                replicationFactor, bootstrap, context.getBrokerConfiguration());
        serviceContainer
            .createService(SYSTEM_PARTITION_BOOTSTRAP_EXPECTED_SERVICE_NAME, bootstrapExpectService)
            .dependency(
                TOPOLOGY_MANAGER_SERVICE, bootstrapExpectService.getTopologyManagerInjector())
            .install();
      }
    }
  }

  private void initClusterOrchestrationLayer(final ServiceContainer serviceContainer) {
    final ClusterOrchestrationInstallService clusterOrchestrationInstallService =
        new ClusterOrchestrationInstallService(serviceContainer);

    serviceContainer
        .createService(
            CLUSTER_ORCHESTRATION_INSTALL_SERVICE_NAME, clusterOrchestrationInstallService)
        .dependency(
            CONTROL_MESSAGE_HANDLER_MANAGER,
            clusterOrchestrationInstallService.getControlMessageHandlerManagerInjector())
        .dependency(
            serverTransport(CLIENT_API_SERVER_NAME),
            clusterOrchestrationInstallService.getTransportInjector())
        .groupReference(
            LEADER_PARTITION_SYSTEM_GROUP_NAME,
            clusterOrchestrationInstallService.getSystemLeaderGroupReference())
        .install();
  }
}
