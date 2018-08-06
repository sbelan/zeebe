package io.zeebe.broker.exporter;

import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.sched.Actor;

import static io.zeebe.broker.exporter.ExporterServiceNames.EXPORTER_MANAGER_SERVICE_NAME;
import static io.zeebe.broker.exporter.ExporterServiceNames.exporterServiceName;

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
