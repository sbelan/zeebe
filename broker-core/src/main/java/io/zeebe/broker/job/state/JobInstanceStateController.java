/*
 * Zeebe Broker Core Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.job.state;

import static io.zeebe.util.StringUtil.getBytes;

import io.zeebe.broker.logstreams.processor.KeyGenerator;
import io.zeebe.logstreams.state.StateController;
import io.zeebe.util.LangUtil;
import java.nio.ByteBuffer;
import java.util.*;
import org.agrona.concurrent.UnsafeBuffer;
import org.rocksdb.*;

public class JobInstanceStateController extends StateController {

  private static final byte[] LATEST_JOB_KEY_BUFFER = getBytes("latestJobKey");

  private final ByteBuffer nullValue = ByteBuffer.allocate(0);

  private final ByteBuffer dbLongBuffer = ByteBuffer.allocate(Long.BYTES);
  private final ByteBuffer dbShortBuffer = ByteBuffer.allocate(Short.BYTES);

  private final UnsafeBuffer keyView = new UnsafeBuffer(0, 0);

  private ColumnFamilyHandle defaultColumnFamilyHandle;
  private ColumnFamilyHandle activatableColumnFamilyHandle;
  private ColumnFamilyHandle activatedColumnFamilyHandle;

  @Override
  protected RocksDB openDB(Options options) throws RocksDBException {

    final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();

    closeables.add(cfOpts);

    final ColumnFamilyDescriptor defaultDescriptor =
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts);
    final ColumnFamilyDescriptor activatableJobsDescriptor =
        new ColumnFamilyDescriptor("activatable-jobs".getBytes(), cfOpts);
    final ColumnFamilyDescriptor activatedJobsDescriptor =
        new ColumnFamilyDescriptor("activated-jobs".getBytes(), cfOpts);

    final List<ColumnFamilyDescriptor> cfDescriptors =
        Arrays.asList(defaultDescriptor, activatableJobsDescriptor, activatedJobsDescriptor);

    final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

    final DBOptions dbOptions = new DBOptions();
    // TODO: figure out dbOptions

    final RocksDB db =
        RocksDB.open(dbOptions, dbDirectory.getAbsolutePath(), cfDescriptors, columnFamilyHandles);

    closeables.add(db);

    for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {

      closeables.add(columnFamilyHandle);

      final ColumnFamilyDescriptor descriptor = columnFamilyHandle.getDescriptor();
      if (descriptor == defaultDescriptor) {
        this.defaultColumnFamilyHandle = columnFamilyHandle;
      } else if (descriptor == activatableJobsDescriptor) {
        this.activatableColumnFamilyHandle = columnFamilyHandle;
      } else if (descriptor == activatedJobsDescriptor) {
        this.activatedColumnFamilyHandle = columnFamilyHandle;
      }
    }

    return db;
  }

  public void putActivatableJob(long key) {
    dbLongBuffer.putLong(0, key);

    try {
      getDb().put(activatableColumnFamilyHandle, dbLongBuffer.array(), nullValue.array());
    } catch (RocksDBException e) {
      LangUtil.rethrowUnchecked(e);
    }
  }

  public long getAndRemoveActivatableJob() {
    try (RocksIterator iterator = getDb().newIterator(activatableColumnFamilyHandle)) {

      iterator.seekToFirst();

      if (iterator.isValid()) {
        final byte[] keyBytes = iterator.key();

        try {
          getDb().delete(activatableColumnFamilyHandle, keyBytes);
        } catch (RocksDBException e) {
          LangUtil.rethrowUnchecked(e);
        }

        keyView.wrap(keyBytes);

        return keyView.getLong(0);
      } else {
        return -1;
      }
    }
  }

  public void recoverLatestJobKey(KeyGenerator keyGenerator) {
    ensureIsOpened("recoverLatestJobKey");

    if (tryGet(LATEST_JOB_KEY_BUFFER, dbLongBuffer.array())) {
      keyGenerator.setKey(dbLongBuffer.getLong(0));
    }
  }

  public void putLatestJobKey(long key) {
    ensureIsOpened("putLatestJobKey");

    dbLongBuffer.putLong(0, key);

    try {
      getDb().put(LATEST_JOB_KEY_BUFFER, dbLongBuffer.array());
    } catch (RocksDBException e) {
      LangUtil.rethrowUnchecked(e);
    }
  }

  public void putJobState(long key, short state) {
    ensureIsOpened("putJobState");

    dbLongBuffer.putLong(0, key);
    dbShortBuffer.putShort(0, state);

    try {
      getDb().put(dbLongBuffer.array(), dbShortBuffer.array());
    } catch (RocksDBException e) {
      LangUtil.rethrowUnchecked(e);
    }
  }

  public short getJobState(long key) {
    ensureIsOpened("getJobState");

    short state = -1;
    dbLongBuffer.putLong(0, key);

    if (tryGet(dbLongBuffer.array(), dbShortBuffer.array())) {
      state = dbShortBuffer.getShort(0);
    }

    return state;
  }

  public void deleteJobState(long key) {
    ensureIsOpened("deleteJobState");

    dbLongBuffer.putLong(0, key);

    try {
      getDb().delete(dbLongBuffer.array());
    } catch (RocksDBException e) {
      LangUtil.rethrowUnchecked(e);
    }
  }

  private boolean tryGet(final byte[] keyBuffer, final byte[] valueBuffer) {
    boolean found = false;

    try {
      final int bytesRead = getDb().get(keyBuffer, valueBuffer);
      found = bytesRead == valueBuffer.length;
    } catch (RocksDBException e) {
      LangUtil.rethrowUnchecked(e);
    }

    return found;
  }
}
