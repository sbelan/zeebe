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
package io.zeebe.broker.exporter.manager;

import static io.zeebe.broker.logstreams.processor.StreamProcessorIds.EXPORTER_MANAGER_PROCESSOR_ID;

import io.netty.util.internal.StringUtil;
import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.exporter.ExporterRecord;
import io.zeebe.broker.logstreams.processor.CommandProcessor;
import io.zeebe.broker.logstreams.processor.StreamProcessorLifecycleAware;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.logstreams.processor.StreamProcessor;
import io.zeebe.logstreams.spi.SnapshotController;
import io.zeebe.logstreams.state.StateSnapshotController;
import io.zeebe.logstreams.state.StateStorage;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.ExporterIntent;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;

public class ExporterManagerProcessor {
  public static final String NAME = StringUtil.simpleClassName(ExporterManagerProcessor.class);
  private static final Logger LOG = Loggers.EXPORTERS;

  private final ExporterManagerState state = new ExporterManagerState();

  public SnapshotController createSnapshotController(final Partition partition) {
    final StateStorage stateStorage =
        partition
            .getStateStorageFactory()
            .create(EXPORTER_MANAGER_PROCESSOR_ID, ExporterManagerProcessor.NAME);
    return new StateSnapshotController(state, stateStorage);
  }

  public long getPosition(final String exporterId) {
    return state.getPosition(exporterId);
  }

  public StreamProcessor createStreamProcessor(
      TypedStreamEnvironment environment, StreamProcessorLifecycleAware listener) {
    return environment
        .newStreamProcessor()
        .onCommand(ValueType.EXPORTER, ExporterIntent.COMMIT, new CommitMessageProcessor())
        .withStateController(state)
        .withListener(listener)
        .build();
  }

  private class CommitMessageProcessor implements CommandProcessor<ExporterRecord> {
    @Override
    public void onCommand(TypedRecord<ExporterRecord> command, CommandControl commandControl) {
      final ExporterRecord record = command.getValue();
      final long currentPosition = state.getPosition(record.getId());

      if (currentPosition >= record.getPosition()) {
        LOG.debug(
            "Tried committing lower position than current, {} < {}",
            record.getPosition(),
            currentPosition);
        commandControl.reject(
            RejectionType.BAD_VALUE, "cannot commit previously committed position");
      }

      try {
        state.setPosition(record.getId(), record.getPosition());
        commandControl.accept(ExporterIntent.COMMITTED);
      } catch (final RocksDBException e) {
        LOG.error("Error updating exporter manager state", e);
        commandControl.reject(RejectionType.PROCESSING_ERROR, "error updating state");
      }
    }
  }
}
