package io.zeebe.broker.exporter;

import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.sched.Actor;

public class ExporterManagerService extends Actor implements Service<ExporterManager> {
  private final ExporterManager manager;

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
}
