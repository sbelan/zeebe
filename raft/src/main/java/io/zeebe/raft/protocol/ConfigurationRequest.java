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
package io.zeebe.raft.protocol;

import static io.zeebe.raft.ConfigurationRequestEncoder.nodeIdNullValue;
import static io.zeebe.raft.ConfigurationRequestEncoder.partitionIdNullValue;
import static io.zeebe.raft.ConfigurationRequestEncoder.termNullValue;

import io.zeebe.logstreams.log.LogStream;
import io.zeebe.raft.BooleanType;
import io.zeebe.raft.ConfigurationRequestDecoder;
import io.zeebe.raft.ConfigurationRequestEncoder;
import io.zeebe.raft.Raft;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class ConfigurationRequest extends AbstractRaftMessage
    implements HasNodeId, HasTerm, HasPartition {

  protected final ConfigurationRequestDecoder bodyDecoder = new ConfigurationRequestDecoder();
  protected final ConfigurationRequestEncoder bodyEncoder = new ConfigurationRequestEncoder();

  protected int partitionId;
  protected int term;
  protected long nodeId;
  protected boolean joinRequest;

  public ConfigurationRequest() {
    reset();
  }

  public ConfigurationRequest reset() {
    partitionId = partitionIdNullValue();
    term = termNullValue();
    nodeId = nodeIdNullValue();
    joinRequest = true;

    return this;
  }

  @Override
  protected int getVersion() {
    return bodyDecoder.sbeSchemaVersion();
  }

  @Override
  protected int getSchemaId() {
    return bodyDecoder.sbeSchemaId();
  }

  @Override
  protected int getTemplateId() {
    return bodyDecoder.sbeTemplateId();
  }

  @Override
  public int getPartitionId() {
    return partitionId;
  }

  @Override
  public int getTerm() {
    return term;
  }

  public boolean isJoinRequest() {
    return joinRequest;
  }

  @Override
  public int getNodeId() {
    return (int) nodeId;
  }

  public ConfigurationRequest setRaft(final Raft raft) {
    final LogStream logStream = raft.getLogStream();

    partitionId = logStream.getPartitionId();
    term = raft.getTerm();
    nodeId = raft.getNodeId();

    return this;
  }

  public ConfigurationRequest setLeave() {
    joinRequest = false;
    return this;
  }

  @Override
  public int getLength() {
    return headerEncoder.encodedLength() + bodyEncoder.sbeBlockLength();
  }

  @Override
  public void wrap(final DirectBuffer buffer, int offset, final int length) {
    reset();

    final int frameEnd = offset + length;

    headerDecoder.wrap(buffer, offset);
    offset += headerDecoder.encodedLength();

    bodyDecoder.wrap(buffer, offset, headerDecoder.blockLength(), headerDecoder.version());

    partitionId = bodyDecoder.partitionId();
    term = bodyDecoder.term();
    nodeId = bodyDecoder.nodeId();
    joinRequest = bodyDecoder.join() == BooleanType.TRUE;

    assert bodyDecoder.limit() == frameEnd
        : "Decoder read only to position "
            + bodyDecoder.limit()
            + " but expected "
            + frameEnd
            + " as final position";
  }

  @Override
  public void write(final MutableDirectBuffer buffer, int offset) {
    headerEncoder
        .wrap(buffer, offset)
        .blockLength(bodyEncoder.sbeBlockLength())
        .templateId(bodyEncoder.sbeTemplateId())
        .schemaId(bodyEncoder.sbeSchemaId())
        .version(bodyEncoder.sbeSchemaVersion());

    offset += headerEncoder.encodedLength();

    bodyEncoder
        .wrap(buffer, offset)
        .partitionId(partitionId)
        .term(term)
        .nodeId(nodeId)
        .join(joinRequest ? BooleanType.TRUE : BooleanType.FALSE);
  }
}
