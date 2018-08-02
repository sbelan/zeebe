package io.zeebe.broker.exporter;

import io.zeebe.exporter.spi.Context;
import io.zeebe.exporter.spi.Exporter;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.sched.Actor;

public class ExporterService extends Actor implements Service<Exporter> {
  private final String name;
  private final Exporter exporter;

  public ExporterService(final Exporter exporter, final String name) {
    this.exporter = exporter;
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  protected void onActorStarting() {
    final Context context = new ExporterContext();
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
  public void stop(ServiceStopContext stopContext) {}

  @Override
  public Exporter get() {
    return exporter;
  }
}
