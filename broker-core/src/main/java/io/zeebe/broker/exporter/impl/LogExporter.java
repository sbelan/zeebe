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

import io.zeebe.exporter.spi.Configuration;
import io.zeebe.exporter.spi.Context;
import io.zeebe.exporter.spi.Event;
import io.zeebe.exporter.spi.Exporter;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class LogExporter implements Exporter, Configuration {
  private static final Future<Void> FINISHED = CompletableFuture.completedFuture(null);
  private static final int BATCH_SIZE = 100;
  private static final Duration BATCH_FLUSH_TIMEOUT = Duration.ofSeconds(1);

  private Context context;

  @Override
  public Configuration getConfiguration() {
    return this;
  }

  @Override
  public void start(Context context) {
    context.getLogger().info("Starting exporter {}", context.getId());
  }

  @Override
  public void stop() {
    context.getLogger().info("Stopping exporter {}", context.getId());
  }

  @Override
  public void export(Event event) {
    context.getLogger().info("Exporter {} exporting {}", context.getId(), event);
    context.getLastPositionUpdater().accept(event.getPosition());
  }

  @Override
  public int getBatchSize() {
    return BATCH_SIZE;
  }

  @Override
  public Duration getBatchFlushTimeout() {
    return BATCH_FLUSH_TIMEOUT;
  }
}
