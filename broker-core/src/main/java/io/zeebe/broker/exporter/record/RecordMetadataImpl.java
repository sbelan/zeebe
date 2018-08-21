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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.RecordMetadata;
import io.zeebe.protocol.intent.Intent;
import io.zeebe.util.LangUtil;
import io.zeebe.util.buffer.BufferUtil;
import java.time.Instant;

@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.PUBLIC_ONLY)
@JsonInclude(Include.NON_NULL)
public class RecordMetadataImpl implements io.zeebe.exporter.record.RecordMetadata {
  private static final ObjectWriter JSON_WRITER =
      new ObjectMapper().writerFor(RecordMetadataImpl.class);

  private final int partitionId;
  private final LoggedEvent event;
  private final RecordMetadata metadata;

  private Instant timestamp;
  private String rejectionReason;

  public RecordMetadataImpl(
      final int partitionId, final LoggedEvent event, final RecordMetadata metadata) {
    this.partitionId = partitionId;
    this.event = event;
    this.metadata = metadata;
  }

  @Override
  public long getKey() {
    return event.getKey();
  }

  @Override
  public Intent getIntent() {
    return metadata.getIntent();
  }

  @Override
  public int getPartitionId() {
    return partitionId;
  }

  @Override
  public long getPosition() {
    return event.getPosition();
  }

  @Override
  public RecordType getRecordType() {
    return metadata.getRecordType();
  }

  @Override
  public RejectionType getRejectionType() {
    return metadata.getRejectionType();
  }

  @Override
  public String getRejectionReason() {
    if (rejectionReason == null) {
      rejectionReason = BufferUtil.bufferAsString(metadata.getRejectionReason());
    }

    return rejectionReason;
  }

  @Override
  public long getSourceRecordPosition() {
    return event.getSourceEventPosition();
  }

  @Override
  public Instant getTimestamp() {
    if (timestamp == null) {
      timestamp = Instant.ofEpochMilli(event.getTimestamp());
    }

    return timestamp;
  }

  @Override
  public ValueType getValueType() {
    return metadata.getValueType();
  }

  @Override
  public String toJson() {
    String json = "";

    try {
      json = JSON_WRITER.writeValueAsString(this);
    } catch (final JsonProcessingException e) {
      LangUtil.rethrowUnchecked(e);
    }

    return json;
  }
}
