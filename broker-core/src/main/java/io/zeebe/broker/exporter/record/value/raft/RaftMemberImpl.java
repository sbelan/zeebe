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
package io.zeebe.broker.exporter.record.value.raft;

import io.zeebe.exporter.record.value.raft.RaftMember;
import io.zeebe.raft.event.RaftConfigurationEventMember;
import io.zeebe.util.buffer.BufferUtil;

public class RaftMemberImpl implements RaftMember {
  private final RaftConfigurationEventMember raftMember;

  private String host;

  public RaftMemberImpl(final RaftConfigurationEventMember raftMember) {
    this.raftMember = raftMember;
  }

  @Override
  public String getHost() {
    if (host == null) {
      host = BufferUtil.bufferAsString(raftMember.getHost());
    }

    return host;
  }

  @Override
  public int getPort() {
    return raftMember.getPort();
  }
}
