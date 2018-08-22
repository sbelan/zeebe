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
package io.zeebe.broker.job.state;

import static io.zeebe.util.StringUtil.getBytes;

import io.zeebe.broker.logstreams.processor.KeyGenerator;
import io.zeebe.logstreams.state.StateController;
import java.nio.ByteBuffer;

public class JobInstanceStateController extends StateController {
  private static final byte[] LATEST_JOB_KEY_BUFFER = getBytes("latestJobKey");
  private final ByteBuffer dbLongBuffer = ByteBuffer.allocate(Long.BYTES);
  private final ByteBuffer dbShortBuffer = ByteBuffer.allocate(Short.BYTES);

  public void recoverLatestJobKey(KeyGenerator keyGenerator) {
    ensureIsOpened("recoverLatestJobKey");

    if (tryGet(LATEST_JOB_KEY_BUFFER, dbLongBuffer.array())) {
      keyGenerator.setKey(dbLongBuffer.getLong(0));
    }
  }

  public void putLatestJobKey(long key) {
    ensureIsOpened("putLatestJobKey");

    dbLongBuffer.putLong(0, key);
    put(LATEST_JOB_KEY_BUFFER, dbLongBuffer.array());
  }

  public void putJobState(long key, short state) {
    ensureIsOpened("putJobState");

    dbShortBuffer.putShort(0, state);
    put(key, dbShortBuffer.array());
  }

  public short getJobState(long key) {
    ensureIsOpened("getJobState");

    short state = -1;
    if (tryGet(key, dbShortBuffer.array())) {
      state = dbShortBuffer.getShort(0);
    }

    return state;
  }

  public void deleteJobState(long key) {
    ensureIsOpened("deleteJobState");

    delete(key);
  }
}
