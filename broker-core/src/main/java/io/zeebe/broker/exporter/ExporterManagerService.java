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
package io.zeebe.broker.exporter;

import static io.zeebe.broker.exporter.ExporterServiceNames.EXPORTER_MANAGER_SERVICE_NAME;
import static io.zeebe.broker.exporter.ExporterServiceNames.exporterServiceName;

import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.sched.Actor;

public class ExporterManagerService extends Actor implements Service<ExporterManager> {
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
    manager.getLoadedExporters().forEach(this::install);
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

  public void install(final ExporterDescriptor descriptor) {
    final Service<ExporterDescriptor> service = new ExporterService(descriptor);
    final ServiceName<ExporterDescriptor> serviceName = exporterServiceName(descriptor.getId());

    serviceStartContext
        .createService(serviceName, service)
        .dependency(EXPORTER_MANAGER_SERVICE_NAME)
        .install();
  }
}
