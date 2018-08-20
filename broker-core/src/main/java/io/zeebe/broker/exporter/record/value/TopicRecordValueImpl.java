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

import io.zeebe.broker.clustering.orchestration.topic.TopicRecord;
import io.zeebe.broker.exporter.record.RecordValueImpl;
import io.zeebe.exporter.record.value.TopicRecordValue;
import java.util.ArrayList;
import java.util.List;

public class TopicRecordValueImpl extends RecordValueImpl implements TopicRecordValue {
  private final TopicRecord record;

  private String name;
  private List<Integer> partitionIds;

  public TopicRecordValueImpl(final TopicRecord record) {
    super(record);
    this.record = record;
  }

  @Override
  public String getName() {
    if (name == null) {
      name = asString(record.getName());
    }

    return name;
  }

  @Override
  public int getPartitionCount() {
    return record.getPartitions();
  }

  @Override
  public int getReplicationFactor() {
    return record.getReplicationFactor();
  }

  @Override
  public List<Integer> getPartitionIds() {
    if (partitionIds == null) {
      partitionIds = new ArrayList<>();
      record.getPartitionIds().forEach(id -> partitionIds.add(id.getValue()));
    }

    return partitionIds;
  }
}
