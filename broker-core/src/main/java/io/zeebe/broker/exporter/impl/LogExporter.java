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
package io.zeebe.broker.exporter.impl;

import io.zeebe.exporter.spi.Context;
import io.zeebe.exporter.spi.Event;
import io.zeebe.exporter.spi.Exporter;

public class LogExporter implements Exporter {
  private Context context;

  @Override
  public void start(Context context) {
    this.context = context;
    this.context.getLogger().info("Starting exporter {}", this.context.getId());
  }

  @Override
  public void stop() {
    context.getLogger().info("Stopping exporter {}", context.getId());
  }

  @Override
  public void export(Event event) {
    context.getLogger().info("Exporter {} exporting {}", context.getId(), event);
    context.updateLastExportedPosition(event.getPosition());
  }
}
