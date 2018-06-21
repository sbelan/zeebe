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
        .dependency(TOPOLOGY_MANAGER_SERVICE, streamProcessorManager.getTopologyManagerInjector())
        .dependency(
            clientTransport(CLIENT_API_CLIENT_NAME),
            streamProcessorManager.getManagementApiClientInjector())
        .dependency(
            STREAM_PROCESSOR_SERVICE_FACTORY,
            streamProcessorManager.getStreamProcessorServiceFactoryInjector())
        .groupReference(
            LEADER_PARTITION_GROUP_NAME, streamProcessorManager.getPartitionsGroupReference())
        .install();
  }
}
