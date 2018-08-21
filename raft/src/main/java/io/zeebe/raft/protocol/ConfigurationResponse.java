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

import static io.zeebe.raft.ConfigurationResponseEncoder.MembersEncoder.sbeBlockLength;
import static io.zeebe.raft.ConfigurationResponseEncoder.MembersEncoder.sbeHeaderSize;
import static io.zeebe.raft.ConfigurationResponseEncoder.termNullValue;

import io.zeebe.raft.BooleanType;
import io.zeebe.raft.ConfigurationResponseDecoder;
import io.zeebe.raft.ConfigurationResponseEncoder;
import io.zeebe.raft.Raft;
import io.zeebe.raft.RaftMember;
import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class ConfigurationResponse extends AbstractRaftMessage implements HasTerm {

  protected final ConfigurationResponseDecoder bodyDecoder = new ConfigurationResponseDecoder();
  protected final ConfigurationResponseEncoder bodyEncoder = new ConfigurationResponseEncoder();

  protected int term;
  protected boolean succeeded;
  protected List<Integer> members = new ArrayList<>();

  public ConfigurationResponse() {
    reset();
  }

  public ConfigurationResponse reset() {
    term = termNullValue();
    succeeded = false;
    members.clear();

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
  public int getTerm() {
    return term;
  }

  public boolean isSucceeded() {
    return succeeded;
  }

  public ConfigurationResponse setSucceeded(final boolean succeeded) {
    this.succeeded = succeeded;
    return this;
  }

  public List<Integer> getMembers() {
    return members;
  }

  public ConfigurationResponse setRaft(final Raft raft) {
    term = raft.getTerm();
    members.add(raft.getNodeId());

    final List<RaftMember> memberList = raft.getRaftMembers().getMemberList();
    memberList.forEach((m) -> members.add(m.getNodeId()));

    return this;
  }

  @Override
  public int getLength() {
    final int membersCount = members.size();

    return headerEncoder.encodedLength()
        + bodyEncoder.sbeBlockLength()
        + sbeHeaderSize()
        + (sbeBlockLength() * membersCount);
  }

  @Override
  public void wrap(final DirectBuffer buffer, int offset, final int length) {
    reset();

    final int frameEnd = offset + length;

    headerDecoder.wrap(buffer, offset);
    offset += headerDecoder.encodedLength();

    bodyDecoder.wrap(buffer, offset, headerDecoder.blockLength(), headerDecoder.version());

    term = bodyDecoder.term();
    succeeded = bodyDecoder.succeeded() == BooleanType.TRUE;

    for (final ConfigurationResponseDecoder.MembersDecoder decoder : bodyDecoder.members()) {
      members.add((int) decoder.nodeId());
    }

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
        .term(term)
        .succeeded(succeeded ? BooleanType.TRUE : BooleanType.FALSE);

    final ConfigurationResponseEncoder.MembersEncoder encoder =
        bodyEncoder.membersCount(members.size());
    for (Integer member : members) {
      encoder.next().nodeId(member);
    }
  }
}
