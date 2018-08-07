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
package io.zeebe.broker.exporter.processor;

import io.zeebe.broker.Loggers;
import io.zeebe.exporter.spi.Exporter;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.logstreams.processor.EventProcessor;
import org.slf4j.Logger;

public class ExporterEventProcessor implements EventProcessor {
  private static final Logger LOG = Loggers.EXPORTERS;

  private final Exporter exporter;
  private final int partitionId;

  private ExporterEvent event;

  public ExporterEventProcessor(final Exporter exporter, final int partitionId) {
    this.exporter = exporter;
    this.partitionId = partitionId;
  }

  public ExporterEventProcessor wrap(final LoggedEvent rawEvent) {
    event = new ExporterEvent(partitionId).wrap(rawEvent);
    return this;
  }

  @Override
  public boolean executeSideEffects() {
    try {
      exporter.export(event);
      return true;
    } catch (final Exception ex) {
      LOG.error("Error exporting event {}", event, ex);
      return false;
    }
  }
}
