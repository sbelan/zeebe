package io.zeebe.broker.exporter;

import io.zeebe.broker.exporter.processor.ExporterStreamProcessor;
import io.zeebe.servicecontainer.ServiceName;

public class ExporterServiceNames {
  public static final ServiceName<ExporterManager> EXPORTER_MANAGER_SERVICE_NAME =
      ServiceName.newServiceName("exporters.manager", ExporterManager.class);

  public static ServiceName<ExporterDescriptor> exporterServiceName(final String id) {
    return ServiceName.newServiceName(
        String.format("exporters.exporter.%s", id), ExporterDescriptor.class);
  }

  public static ServiceName<ExporterStreamProcessor> exporterStreamProcessorServiceName(
      final String id) {
    return ServiceName.newServiceName(
        String.format("exporters.exporter.%s.processor", id), ExporterStreamProcessor.class);
  }
}
