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

import io.zeebe.exporter.spi.*;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.slf4j.Logger;

public class LogExporter implements Exporter, Configuration {
  private static final Future<Void> FINISHED = CompletableFuture.completedFuture(null);
  private static final int BATCH_SIZE = 100;
  private static final Duration BATCH_FLUSH_TIMEOUT = Duration.ofSeconds(1);

  private Logger logger;
  private String id;

  @Override
  public Future<Configuration> start(Context context) {
    id = context.getId();
    logger = context.getLogger();
    logger.info("Starting exporter {}", id);

    return CompletableFuture.completedFuture(this);
  }

  @Override
  public Future<Void> stop() {
    logger.info("Stopping exporter {}", id);

    return FINISHED;
  }

  @Override
  public void exporter(Batch batch) {
    for (Event event : batch) {
      logger.info("Processed {}", event.toString());
    }

    batch.commit();
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
