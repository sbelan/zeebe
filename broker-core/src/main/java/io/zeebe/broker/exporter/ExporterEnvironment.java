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

import io.zeebe.broker.Loggers;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;

public class ExporterEnvironment {
  private final Logger logger;
  private final Path workingDirectory;

  public ExporterEnvironment() {
    this.logger = Loggers.EXPORTERS;
    this.workingDirectory = Paths.get(".").normalize();
  }

  public ExporterEnvironment(final Logger logger, final Path workingDirectory) {
    this.logger = logger;
    this.workingDirectory = workingDirectory;
  }

  public Logger getLogger() {
    return logger;
  }

  public Path getWorkingDirectory() {
    return workingDirectory;
  }
}
