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
package io.zeebe.broker.exporter.manager;

import static io.zeebe.broker.logstreams.processor.StreamProcessorIds.EXPORTER_MANAGER_PROCESSOR_ID;
import static io.zeebe.broker.logstreams.processor.StreamProcessorIds.EXPORTER_PROCESSOR_ID;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.exporter.ExporterDescriptor;
import io.zeebe.broker.exporter.processor.ExporterStreamProcessor;
import io.zeebe.broker.logstreams.processor.StreamProcessorLifecycleAware;
import io.zeebe.broker.logstreams.processor.StreamProcessorServiceFactory;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.logstreams.processor.TypedStreamProcessor;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.servicecontainer.*;
import java.util.Set;
import org.slf4j.Logger;

public class ExporterManagerService
    implements Service<ExporterManager>, StreamProcessorLifecycleAware {
  private static final Logger LOG = Loggers.EXPORTERS;
  private final Injector<StreamProcessorServiceFactory> streamProcessorServiceFactoryInjector =
      new Injector<>();
  private StreamProcessorServiceFactory streamProcessorServiceFactory;

  // NOTE: for now, strictly leader partitions
  private final ServiceGroupReference<Partition> partitionsReference =
      ServiceGroupReference.<Partition>create().onAdd(this::installManager).build();

  private final ExporterManager manager;
  private ServiceStartContext serviceStartContext;

  public ExporterManagerService(final ExporterManager manager) {
    this.manager = manager;
  }

  @Override
  public void start(ServiceStartContext startContext) {
    serviceStartContext = startContext;
    streamProcessorServiceFactory = streamProcessorServiceFactoryInjector.getValue();
  }

  public Injector<StreamProcessorServiceFactory> getStreamProcessorServiceFactoryInjector() {
    return streamProcessorServiceFactoryInjector;
  }

  public ServiceGroupReference<Partition> getPartitionsReference() {
    return partitionsReference;
  }

  @Override
  public void stop(ServiceStopContext stopContext) {}

  @Override
  public ExporterManager get() {
    return manager;
  }

  private void installManager(final ServiceName<Partition> name, final Partition partition) {
    final TypedStreamEnvironment env = new TypedStreamEnvironment(partition.getLogStream(), null);
    final ExporterManagerProcessorWrapper wrapper = new ExporterManagerProcessorWrapper();

    streamProcessorServiceFactory
        .createService(partition, name)
        .processor(
            wrapper.createStreamProcessor(
                env,
                new StreamProcessorLifecycleAware() {
                  @Override
                  public void onRecovered(TypedStreamProcessor streamProcessor) {
                    installExporters(name, partition, wrapper);
                  }
                }))
        .processorId(EXPORTER_MANAGER_PROCESSOR_ID)
        .processorName(ExporterManagerProcessorWrapper.NAME)
        .snapshotController(wrapper.createSnapshotController(partition))
        .additionalDependencies(name)
        .build();

    LOG.info("Installed manager stream processor");
  }

  private void installExporters(
      final ServiceName<Partition> name,
      final Partition partition,
      final ExporterManagerProcessorWrapper wrapper) {
    final TypedStreamEnvironment env = new TypedStreamEnvironment(partition.getLogStream(), null);
    final Set<ExporterDescriptor> descriptors = manager.getLoadedExporters();
    for (final ExporterDescriptor descriptor : descriptors) {
      installExporter(name, partition, wrapper, descriptor);
    }

    LOG.info("Installed {} exporter stream processors", descriptors.size());
  }

  private void installExporter(
      ServiceName<Partition> name,
      Partition partition,
      ExporterManagerProcessorWrapper wrapper,
      ExporterDescriptor descriptor) {
    final long position = wrapper.getPosition(descriptor.getId());

    streamProcessorServiceFactory
        .createService(partition, name)
        .processor(
            new ExporterStreamProcessor(descriptor, partition.getInfo().getPartitionId(), position))
        .processorId(EXPORTER_PROCESSOR_ID)
        .processorName(descriptor.getName())
        .eventFilter(e -> e.getValueType() != ValueType.EXPORTER)
        .additionalDependencies(name)
        .build();

    LOG.info("Installed exporter stream processor {}", descriptor.getName());
  }
}
