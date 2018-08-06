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

import io.zeebe.exporter.spi.Context;
import java.nio.file.Path;
import java.util.Map;
import org.slf4j.Logger;

public class ExporterContext implements Context {
  private final String id;
  private final Map<String, Object> args;
  private final ExporterEnvironment env;

  public ExporterContext(
      final String id, final Map<String, Object> args, final ExporterEnvironment env) {
    this.id = id;
    this.args = args;
    this.env = env;
  }

  @Override
  public Logger getLogger() {
    return env.getLogger();
  }

  @Override
  public Path getWorkingDirectory() {
    return env.getWorkingDirectory();
  }

  @Override
  public Map<String, Object> getArgs() {
    return args;
  }

  @Override
  public String getId() {
    return id;
  }
}
