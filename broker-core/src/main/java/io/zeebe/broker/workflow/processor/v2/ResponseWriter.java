package io.zeebe.broker.workflow.processor.v2;

import java.nio.charset.StandardCharsets;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.transport.clientapi.CommandResponseWriter;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.impl.RecordMetadata;
import io.zeebe.protocol.intent.Intent;
import io.zeebe.transport.ServerOutput;

public class ResponseWriter {


  protected CommandResponseWriter writer;
  protected int partitionId;

  private final UnsafeBuffer stringWrapper = new UnsafeBuffer(0, 0);

  private boolean isStaged = false;
  private long requestId;
  private int requestStreamId;

  public ResponseWriter(ServerOutput output, int partitionId) {
    this.writer = new CommandResponseWriter(output);
    this.partitionId = partitionId;
  }

  public void sendAccept(TypedRecord<?> record, Intent intent)
  {
    stringWrapper.wrap(0, 0);
    prepareResponse(RecordType.EVENT, intent, RejectionType.NULL_VAL, stringWrapper, record);
  }

  public void sendReject(TypedRecord<?> record, RejectionType rejectionType, String rejectionReason)
  {
    final byte[] bytes = rejectionReason.getBytes(StandardCharsets.UTF_8);
    stringWrapper.wrap(bytes);
    prepareResponse(RecordType.COMMAND_REJECTION, record.getMetadata().getIntent(), rejectionType, stringWrapper, record);
  }

  private void prepareResponse(
      RecordType type,
      Intent intent,
      RejectionType rejectionType,
      DirectBuffer rejectionReason,
      TypedRecord<?> record)
  {
    final RecordMetadata metadata = record.getMetadata();

    // this differentiation is required because we do not always send a response on the command
    // => https://github.com/zeebe-io/zeebe/issues/1040
    final boolean basedOnCommand = record.getMetadata().getRecordType() == RecordType.COMMAND;

    writer
        .partitionId(partitionId)
        .position(basedOnCommand ? 0 : record.getPosition()) // TODO: this depends on the value of written event =>
        // https://github.com/zeebe-io/zeebe/issues/374
        .sourcePosition(basedOnCommand ? record.getPosition() : record.getSourcePosition())
        .key(record.getKey())
        .timestamp(record.getTimestamp())
        .intent(intent)
        .recordType(type)
        .valueType(metadata.getValueType())
        .rejectionType(rejectionType)
        .rejectionReason(rejectionReason)
        .valueWriter(record.getValue());

    isStaged = true;
    requestId = metadata.getRequestId();
    requestStreamId = metadata.getRequestStreamId();
  }

  public boolean flush()
  {
    if (isStaged)
    {
      return writer.tryWriteResponse(requestStreamId, requestId);
    }
    else
    {
      return true;
    }
  }

  public void reset()
  {

  }


}
