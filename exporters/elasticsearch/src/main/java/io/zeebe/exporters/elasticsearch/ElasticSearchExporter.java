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
import java.util.HashMap;
import java.util.Map;

public class ElasticSearchExporter implements Exporter {
  @Argument private String host = "localhost";
  @Argument private int port = 9200;
  @Argument private String scheme = "http";
  @Argument private int batchSize = 100;

  private Context context;
  private RestHighLevelClient client;

  private final BulkRequest bulkRequest = new BulkRequest();
  private long lastPosition = -1L;

  public void start(Context context) {
    RestHighLevelClient client =
        new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, scheme)));

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

  private void flush() {
    try {
      client.bulk(bulkRequest);
      context.getLastPositionUpdater().accept(lastPosition);
    } catch (IOException e) {
      context.getLogger().error("Failed to write bulk request", e);
    }
  }
}
