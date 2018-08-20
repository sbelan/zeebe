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

import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.RecordMetadata;
import io.zeebe.exporter.record.RecordValue;

public class ExporterRecord<T extends RecordValue> implements Record<T> {
  private final ExporterRecordMetadata metadata;
  private final T value;

  public ExporterRecord(final ExporterRecordMetadata metadata, T value) {
    this.metadata = metadata;
    this.value = value;
  }

  @Override
  public RecordMetadata getMetadata() {
    return metadata;
  }

  @Override
  public T getValue() {
    return value;
  }

  @Override
  public String toJson() throws Exception {
    return "{" + "\"metadata\": " + metadata.toJson() + ",\"value\": " + value.toJson() + "}";
  }
}
