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
