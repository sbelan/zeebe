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
package io.zeebe.broker.system.workflow.repository.processor;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

import io.zeebe.util.buffer.BufferReader;
import io.zeebe.util.buffer.BufferWriter;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class PendingDeploymentDistribution implements BufferReader, BufferWriter {

  private static final int POSITION_OFFSET = 0;
  private static final int DEPLOYMENT_LENGTH_OFFSET = SIZE_OF_LONG;
  private static final int DEPLOYMENT_OFFSET = DEPLOYMENT_LENGTH_OFFSET + SIZE_OF_INT;

  private DirectBuffer deployment;
  private long sourcePosition;
  private long distributionCount;

  public PendingDeploymentDistribution(DirectBuffer deployment, long sourcePosition) {
    this.deployment = deployment;
    this.sourcePosition = sourcePosition;
  }

  public void setDistributionCount(long distributionCount) {
    this.distributionCount = distributionCount;
  }

  public long decrementCount() {
    return --distributionCount;
  }

  //  public void complete() {
  //    pushFuture.complete(null);
  //  }

  public DirectBuffer getDeployment() {
    return deployment;
  }

  public long getSourcePosition() {
    return sourcePosition;
  }
  //
  //  public byte[] toBytes() {
  //    final int deploymentSize = deployment.capacity();
  //    final int length = DEPLOYMENT_OFFSET + deploymentSize;
  //    final byte[] bytes = new byte[length];
  //
  //    final UnsafeBuffer buffer = new UnsafeBuffer(bytes);
  //    buffer.putLong(POSITION_OFFSET, sourcePosition);
  //    buffer.putInt(DEPLOYMENT_LENGTH_OFFSET, deploymentSize);
  //    buffer.putBytes(DEPLOYMENT_OFFSET, deployment, 0, deploymentSize);
  //
  //    return bytes;
  //  }

  @Override
  public void wrap(DirectBuffer buffer, int offset, int length) {
    this.sourcePosition = buffer.getLong(POSITION_OFFSET);
    final int deploymentSize = buffer.getInt(DEPLOYMENT_LENGTH_OFFSET);

    //    buffer.wrap(buffer, DEPLOYMENT_OFFSET, deploymentSize);
    deployment.wrap(buffer, DEPLOYMENT_OFFSET, deploymentSize);
  }

  @Override
  public int getLength() {
    final int deploymentSize = deployment.capacity();
    final int length = DEPLOYMENT_OFFSET + deploymentSize;
    return length;
  }

  @Override
  public void write(MutableDirectBuffer buffer, int offset) {
    final int deploymentSize = deployment.capacity();

    buffer.putLong(offset + POSITION_OFFSET, sourcePosition);
    buffer.putInt(offset + DEPLOYMENT_LENGTH_OFFSET, deploymentSize);
    buffer.putBytes(offset + DEPLOYMENT_OFFSET, deployment, 0, deploymentSize);
  }
}
