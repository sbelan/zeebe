package io.zeebe.broker.exporter;

import io.zeebe.broker.system.Component;
import io.zeebe.broker.system.SystemContext;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.servicecontainer.ServiceName;

public class ExporterComponent implements Component {
  private static final ServiceName<ExporterManager> SERVICE_NAME =
      ServiceName.newServiceName("broker.exporters.manager", ExporterManager.class);

  @Override
  public void init(SystemContext context) {
    final BrokerCfg brokerCfg = context.getBrokerConfiguration();
    final ExporterEnvironment env = new ExporterEnvironment();
    final ExporterManager manager = new ExporterManager(env);

    // Add any internal exporters here
    manager.load("io.zeebe.broker.exporter.LogExporter", LogExporter.class);
    manager.load(brokerCfg.getExporters());

    // TODO: should we use context.requiredStartActions and allow exporters to verify their
    //       configurations are valid? give them a chance to fail early?

    final ExporterManagerService service = new ExporterManagerService(manager);
    context.getServiceContainer().createService(SERVICE_NAME, service).install();
  }
}
