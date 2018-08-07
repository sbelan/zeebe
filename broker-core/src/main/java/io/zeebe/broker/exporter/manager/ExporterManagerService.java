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

import static io.zeebe.broker.exporter.ExporterServiceNames.EXPORTER_MANAGER_SERVICE_NAME;
import static io.zeebe.broker.logstreams.processor.StreamProcessorIds.EXPORTER_MANAGER_PROCESSOR_ID;
import static io.zeebe.broker.logstreams.processor.StreamProcessorIds.EXPORTER_PROCESSOR_ID;

import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.exporter.ExporterDescriptor;
import io.zeebe.broker.exporter.processor.ExporterStreamProcessor;
import io.zeebe.broker.logstreams.processor.StreamProcessorLifecycleAware;
import io.zeebe.broker.logstreams.processor.StreamProcessorServiceFactory;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.logstreams.processor.TypedStreamProcessor;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.servicecontainer.*;
import io.zeebe.util.sched.Actor;
import java.util.Set;

public class ExporterManagerService extends Actor
    implements Service<ExporterManager>, StreamProcessorLifecycleAware {
  private final Injector<StreamProcessorServiceFactory> streamProcessorServiceFactoryInjector =
      new Injector<>();
  private StreamProcessorServiceFactory streamProcessorServiceFactory;

  // NOTE: for now, strictly leader partitions
  private final ServiceGroupReference<Partition> partitionsReference =
      ServiceGroupReference.<Partition>create().onAdd(this::installManagerStreamProcessor).build();

  private final ExporterManager manager;
  private ServiceStartContext serviceStartContext;

  public ExporterManagerService(final ExporterManager manager) {
    this.manager = manager;
  }

  @Override
  protected void onActorStarting() {
    super.onActorStarting();
  }

  @Override
  protected void onActorStarted() {
    super.onActorStarted();
  }

  @Override
  protected void onActorClosing() {
    super.onActorClosing();
  }

  @Override
  protected void onActorClosed() {
    super.onActorClosed();
  }

  @Override
  public void start(ServiceStartContext startContext) {
    serviceStartContext = startContext;
    streamProcessorServiceFactory = streamProcessorServiceFactoryInjector.getValue();

    startContext.async(startContext.getScheduler().submitActor(this));
  }

  @Override
  public void stop(ServiceStopContext stopContext) {
    stopContext.async(actor.close());
  }

  @Override
  public ExporterManager get() {
    return manager;
  }

  private void installManagerStreamProcessor(
      final ServiceName<Partition> name, final Partition partition) {
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
                    installExporterStreamProcessors(name, partition, wrapper);
                  }
                }))
        .processorId(EXPORTER_MANAGER_PROCESSOR_ID)
        .processorName(ExporterManagerProcessorWrapper.NAME)
        .additionalDependencies(EXPORTER_MANAGER_SERVICE_NAME)
        .build();
  }

  private void installExporterStreamProcessors(
      final ServiceName<Partition> name,
      final Partition partition,
      final ExporterManagerProcessorWrapper wrapper) {
    final TypedStreamEnvironment env = new TypedStreamEnvironment(partition.getLogStream(), null);
    final Set<ExporterDescriptor> descriptors = manager.getLoadedExporters();
    for (final ExporterDescriptor descriptor : descriptors) {
      final long position = wrapper.getPosition(descriptor.getId());

      streamProcessorServiceFactory
          .createService(partition, name)
          .processor(
              new ExporterStreamProcessor(
                  descriptor, partition.getInfo().getPartitionId(), position))
          .processorId(EXPORTER_PROCESSOR_ID)
          .processorName(descriptor.getName())
          .eventFilter(e -> e.getValueType() != ValueType.EXPORTER)
          .build();
    }
  }
}
