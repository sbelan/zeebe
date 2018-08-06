package io.zeebe.broker.exporter;

import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.util.sched.Actor;

public class ExporterService extends Actor implements Service<ExporterDescriptor> {
  private final ExporterDescriptor descriptor;

  public ExporterService(final ExporterDescriptor descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public String getName() {
    return descriptor.getName();
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
  public void start(ServiceStartContext startContext) {}

  @Override
  public ExporterDescriptor get() {
    return descriptor;
  }
}
