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
package io.zeebe.broker.exporter.record;

import io.zeebe.broker.clustering.orchestration.id.IdRecord;
import io.zeebe.broker.clustering.orchestration.topic.TopicRecord;
import io.zeebe.broker.exporter.record.value.DeploymentRecordValueImpl;
import io.zeebe.broker.exporter.record.value.IdRecordValueImpl;
import io.zeebe.broker.exporter.record.value.IncidentRecordValueImpl;
import io.zeebe.broker.exporter.record.value.JobRecordValueImpl;
import io.zeebe.broker.exporter.record.value.MessageRecordValueImpl;
import io.zeebe.broker.exporter.record.value.MessageSubscriptionRecordValueImpl;
import io.zeebe.broker.exporter.record.value.RaftRecordValueImpl;
import io.zeebe.broker.exporter.record.value.TopicRecordValueImpl;
import io.zeebe.broker.exporter.record.value.WorkflowInstanceRecordValueImpl;
import io.zeebe.broker.exporter.record.value.WorkflowInstanceSubscriptionRecordValueImpl;
import io.zeebe.broker.incident.data.IncidentRecord;
import io.zeebe.broker.job.data.JobRecord;
import io.zeebe.broker.subscription.message.data.MessageRecord;
import io.zeebe.broker.subscription.message.data.MessageSubscriptionRecord;
import io.zeebe.broker.subscription.message.data.WorkflowInstanceSubscriptionRecord;
import io.zeebe.broker.system.workflow.repository.data.DeploymentRecord;
import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.raft.event.RaftConfigurationEvent;
import io.zeebe.util.buffer.BufferReader;

public class RecordValueMapper {
  public RecordImpl map(final LoggedEvent event, final RecordMetadataImpl metadata) {
    final RecordImpl record;

    switch (metadata.getValueType()) {
      case JOB:
        record =
            new RecordImpl<>(
                metadata, new JobRecordValueImpl(readRecordValue(event, new JobRecord())));
        break;
      case RAFT:
        record =
            new RecordImpl<>(
                metadata,
                new RaftRecordValueImpl(readRecordValue(event, new RaftConfigurationEvent())));
        break;
      case DEPLOYMENT:
        record =
            new RecordImpl<>(
                metadata,
                new DeploymentRecordValueImpl(readRecordValue(event, new DeploymentRecord())));
        break;
      case WORKFLOW_INSTANCE:
        record =
            new RecordImpl<>(
                metadata,
                new WorkflowInstanceRecordValueImpl(
                    readRecordValue(event, new WorkflowInstanceRecord())));
        break;
      case INCIDENT:
        record =
            new RecordImpl<>(
                metadata,
                new IncidentRecordValueImpl(readRecordValue(event, new IncidentRecord())));
        break;
      case TOPIC:
        record =
            new RecordImpl<>(
                metadata, new TopicRecordValueImpl(readRecordValue(event, new TopicRecord())));
        break;
      case ID:
        record =
            new RecordImpl<>(
                metadata, new IdRecordValueImpl(readRecordValue(event, new IdRecord())));
        break;
      case MESSAGE:
        record =
            new RecordImpl<>(
                metadata, new MessageRecordValueImpl(readRecordValue(event, new MessageRecord())));
        break;
      case MESSAGE_SUBSCRIPTION:
        record =
            new RecordImpl<>(
                metadata,
                new MessageSubscriptionRecordValueImpl(
                    readRecordValue(event, new MessageSubscriptionRecord())));
        break;
      case WORKFLOW_INSTANCE_SUBSCRIPTION:
        record =
            new RecordImpl<>(
                metadata,
                new WorkflowInstanceSubscriptionRecordValueImpl(
                    readRecordValue(event, new WorkflowInstanceSubscriptionRecord())));
        break;
      default:
        record = null;
        break;
    }

    return record;
  }

  private <T extends BufferReader> T readRecordValue(final LoggedEvent event, final T reader) {
    event.readValue(reader);
    return reader;
  }
}
