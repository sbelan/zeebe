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
import io.zeebe.broker.exporter.ExporterContext;
import io.zeebe.broker.exporter.ExporterDescriptor;
import io.zeebe.broker.exporter.ExporterRecord;
import io.zeebe.broker.logstreams.processor.NoopSnapshotSupport;
import io.zeebe.exporter.spi.Exporter;
import io.zeebe.logstreams.log.LogStreamReader;
import io.zeebe.logstreams.log.LogStreamRecordWriter;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.logstreams.processor.EventProcessor;
import io.zeebe.logstreams.processor.StreamProcessor;
import io.zeebe.logstreams.processor.StreamProcessorContext;
import io.zeebe.logstreams.spi.SnapshotSupport;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.RecordMetadata;
import io.zeebe.protocol.intent.ExporterIntent;
import io.zeebe.util.sched.ActorControl;
import org.slf4j.Logger;

public class ExporterProcessor implements StreamProcessor {
  private static final Logger LOG = Loggers.EXPORTERS;
  private static final SnapshotSupport NONE = new NoopSnapshotSupport();

  private final ExporterRecord record = new ExporterRecord();
  private final RecordMetadata metadata = new RecordMetadata();

  private final Exporter exporter;
  private final ExporterContext exporterContext;
  private final ExporterEventProcessor eventProcessor;
  private final long startPosition;

  private LogStreamRecordWriter writer;
  private ActorControl actor;

  public ExporterProcessor(
      final ExporterDescriptor descriptor, final int partitionId, final long startPosition) {
    this.exporter = descriptor.create();
    this.exporterContext =
        new ExporterContext(
            descriptor.getId(),
            descriptor.getArgs(),
            descriptor.getEnv(),
            this::schedulePositionUpdate);
    this.eventProcessor = new ExporterEventProcessor(exporter, partitionId);
    this.startPosition = startPosition;
  }

  @Override
  public SnapshotSupport getStateResource() {
    return NONE;
  }

  @Override
  public void onOpen(StreamProcessorContext context) {
    if (startPosition >= 0) {
      restartFromPosition(context.getLogStreamReader(), startPosition);
    }

    actor = context.getActorControl();
    writer = context.getLogStreamWriter();
  }

  @Override
  public void onRecovered() {
    exporter.start(exporterContext);
    LOG.info("Recovered!");
  }

  @Override
  public EventProcessor onEvent(LoggedEvent event) {
    return eventProcessor.wrap(event);
  }

  @Override
  public void onClose() {
    exporter.stop();
  }

  private void schedulePositionUpdate(final long position) {
    actor.runUntilDone(
        () -> {
          try {
            commitPosition(position);
            actor.done();
          } catch (final Exception ex) {
            actor.yield();
          }
        });
  }

  private void commitPosition(final long position) {
    writer.reset();
    metadata.reset();

    metadata
        .valueType(ValueType.EXPORTER)
        .intent(ExporterIntent.COMMIT)
        .recordType(RecordType.COMMAND);
    record.setId(exporterContext.getId()).setPosition(position);

    writer.positionAsKey().valueWriter(record).metadataWriter(metadata).tryWrite();
  }

  private void restartFromPosition(final LogStreamReader reader, final long position) {
    if (reader.seek(position)) {
      if (reader.hasNext()) {
        reader.seek(position + 1);
      }
    }
  }
}
