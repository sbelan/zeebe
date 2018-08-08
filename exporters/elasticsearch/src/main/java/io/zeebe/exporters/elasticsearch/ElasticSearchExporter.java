/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.exporters.elasticsearch;

import io.zeebe.exporter.spi.Argument;
import io.zeebe.exporter.spi.Context;
import io.zeebe.exporter.spi.Event;
import io.zeebe.exporter.spi.Exporter;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ElasticSearchExporter implements Exporter {
  @Argument private String host = "localhost";
  @Argument private int port = 9200;
  @Argument private String scheme = "http";

  @Argument("batch_size")
  private int batchSize = 100;

  private Context context;
  private RestHighLevelClient client;

  private final BulkRequest bulkRequest = new BulkRequest();
  private long lastPosition = -1L;

  private Instant lastFlush;
  private Duration flushDelay = Duration.ofSeconds(1);
  private AtomicBoolean flushScheduled = new AtomicBoolean(false);

  public void start(Context context) {
    client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, scheme)));

    this.context = context;
    this.context
        .getLogger()
        .info(
            "Started with config: host={}, port={}, scheme={}, batchSize={}",
            host,
            port,
            scheme,
            batchSize);
  }

  public void stop() {
    try {
      client.close();
    } catch (IOException e) {
      context.getLogger().error("Could not close Elasticsearch client", e);
    }
  }

  public void export(Event event) {
    addToBulkRequest(event);

    if (shouldFlush()) {
      flush();
    } else {
      if (flushScheduled.compareAndSet(false, true)) {
        context.schedule(this::handleScheduledFlush, flushDelay);
      }
    }
  }

  private void addToBulkRequest(final Event event) {
    final Map<String, Object> dict = new HashMap<>();

    dict.put("partitionId", event.getPartitionId());
    dict.put("position", event.getPosition());
    dict.put("sourceRecordPosition", event.getSourceRecordPosition());
    dict.put("timestamp", event.getTimestamp());
    dict.put("key", event.getKey());
    dict.put("intent", event.getIntent());
    dict.put("recordType", event.getRecordType());

    dict.put("rejectionType", event.getRejectionType());
    dict.put("rejectionReason", fromByteBuffer(event.getRejectionReason()));

    dict.put("valueType", event.getValueType());
    dict.put("value", toArray(event.getValue()));

    lastPosition = event.getPosition();
    bulkRequest.add(new UpdateRequest().doc(dict));
    context.getLogger().info("Added {} to bulkRequest", event.getPosition());
  }

  private boolean shouldFlush() {
    return bulkRequest.requests().size() >= batchSize;
  }

  private byte[] toArray(final ByteBuffer buffer) {
    final byte[] bytes = new byte[buffer.capacity()];
    buffer.get(bytes);
    return bytes;
  }

  private String fromByteBuffer(final ByteBuffer buffer) {
    return new String(StandardCharsets.UTF_8.decode(buffer).array());
  }

  private void handleScheduledFlush() {
    if (lastFlush == null
        || Duration.between(Instant.now(), lastFlush).compareTo(flushDelay) >= 0) {
      flushScheduled.set(false);
      if (shouldFlush()) {
        flush();
      }
    }
  }

  private void flush() {
    try {
      int requestsCount = bulkRequest.requests().size();
      client.bulk(bulkRequest);
      context.updateLastExportedPosition(lastPosition);
      lastFlush = Instant.now();
      context.getLogger().info("Exported {} events", requestsCount);
      bulkRequest.requests().clear();
    } catch (IOException e) {
      context.getLogger().error("Failed to write bulk request", e);
    }
  }
}
