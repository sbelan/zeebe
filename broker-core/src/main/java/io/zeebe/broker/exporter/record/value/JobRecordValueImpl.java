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

import io.zeebe.broker.exporter.record.RecordValueWithPayloadImpl;
import io.zeebe.broker.exporter.record.value.job.HeadersImpl;
import io.zeebe.broker.job.data.JobRecord;
import io.zeebe.exporter.record.value.JobRecordValue;
import io.zeebe.exporter.record.value.job.Headers;
import io.zeebe.util.LangUtil;
import io.zeebe.util.buffer.BufferUtil;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;

public class JobRecordValueImpl extends RecordValueWithPayloadImpl implements JobRecordValue {
  private final JobRecord record;

  private String type;
  private String worker;
  private Instant deadline;

  private HeadersImpl headers;
  private Map<String, Object> customHeaders;

  public JobRecordValueImpl(final JobRecord record) {
    super(record, record.getPayload());
    this.record = record;
  }

  @Override
  public String getType() {
    if (type == null) {
      type = BufferUtil.bufferAsString(record.getType());
    }

    return type;
  }

  @Override
  public Headers getHeaders() {
    if (headers == null) {
      headers = new HeadersImpl(record.headers());
    }

    return headers;
  }

  @Override
  public Map<String, Object> getCustomHeaders() {
    if (customHeaders == null) {
      try {
        customHeaders = asMsgPackMap(record.getCustomHeaders());
      } catch (final IOException e) {
        LangUtil.rethrowUnchecked(e);
      }
    }

    return customHeaders;
  }

  @Override
  public String getWorker() {
    if (worker == null) {
      worker = asString(record.getWorker());
    }

    return worker;
  }

  @Override
  public int getRetries() {
    return record.getRetries();
  }

  @Override
  public Instant getDeadline() {
    if (deadline == null) {
      deadline = Instant.ofEpochMilli(record.getDeadline());
    }

    return deadline;
  }
}
