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
package io.zeebe.broker.exporter.record.value;

import io.zeebe.broker.exporter.record.RecordValueImpl;
import io.zeebe.broker.exporter.record.value.raft.RaftMemberImpl;
import io.zeebe.exporter.record.value.RaftRecordValue;
import io.zeebe.exporter.record.value.raft.RaftMember;
import io.zeebe.raft.event.RaftConfigurationEvent;
import java.util.ArrayList;
import java.util.List;

public class RaftRecordValueImpl extends RecordValueImpl implements RaftRecordValue {
  private final RaftConfigurationEvent record;

  private List<RaftMember> members;

  public RaftRecordValueImpl(final RaftConfigurationEvent record) {
    super(record);
    this.record = record;
  }

  @Override
  public List<RaftMember> getMembers() {
    if (members == null) {
      members = new ArrayList<>();
      record.members().forEach(m -> members.add(new RaftMemberImpl(m)));
    }

    return null;
  }
}
