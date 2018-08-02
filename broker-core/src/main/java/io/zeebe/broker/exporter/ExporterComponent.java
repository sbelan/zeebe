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
    final ExporterContext exporterContext = new ExporterContext();

    final ExporterManager manager = new ExporterManager(exporterContext);
    manager.loadExporters(brokerCfg.getExporters());

    final ExporterManagerService service = new ExporterManagerService(manager);
    context.getServiceContainer().createService(SERVICE_NAME, service).install();
  }
}
