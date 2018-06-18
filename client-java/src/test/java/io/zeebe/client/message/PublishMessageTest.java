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
package io.zeebe.client.message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import io.zeebe.client.api.events.MessageEvent;
import io.zeebe.client.api.events.MessageState;
import io.zeebe.client.api.record.RecordMetadata;
import io.zeebe.client.impl.data.MsgPackConverter;
import io.zeebe.client.util.ClientRule;
import io.zeebe.protocol.clientapi.ExecuteCommandRequestEncoder;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.MessageIntent;
import io.zeebe.test.broker.protocol.brokerapi.ExecuteCommandRequest;
import io.zeebe.test.broker.protocol.brokerapi.StubBrokerRule;
import java.time.Instant;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

public class PublishMessageTest {
  private static final String PAYLOAD = "{\"foo\":\"bar\"}";
  private static final byte[] MSGPACK_PAYLOAD = new MsgPackConverter().convertToMsgPack(PAYLOAD);

  public ClientRule clientRule = new ClientRule();
  public StubBrokerRule brokerRule = new StubBrokerRule();

  @Rule public RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(clientRule);

  @Rule public ExpectedException exception = ExpectedException.none();

  private final Instant now = Instant.now();

  @Before
  public void setUp() {
    brokerRule
        .messages()
        .registerPublishCommand(
            c ->
                c.key(123)
                    .position(456)
                    .timestamp(now)
                    .sourceRecordPosition(1L)
                    .value()
                    .allOf(r -> r.getCommand())
                    .done());
  }

  @Test
  public void shouldPublishMessage() {
    // given

    // when
    final MessageEvent message =
        clientRule
            .messageClient()
            .newPublishCommand()
            .messageName("ORDER_CANCELLED")
            .messageKey("some-order-id")
            .payload(PAYLOAD)
            .send()
            .join();

    // then
    final ExecuteCommandRequest request = brokerRule.getReceivedCommandRequests().get(0);
    assertThat(request.valueType()).isEqualTo(ValueType.MESSAGE);
    assertThat(request.intent()).isEqualTo(MessageIntent.PUBLISH);
    assertThat(request.partitionId()).isEqualTo(StubBrokerRule.TEST_PARTITION_ID);
    assertThat(request.position()).isEqualTo(ExecuteCommandRequestEncoder.positionNullValue());

    assertThat(request.getCommand())
        .containsOnly(
            entry("messageName", "ORDER_CANCELLED"),
            entry("messageKey", "some-order-id"),
            entry("payload", MSGPACK_PAYLOAD));

    assertThat(message.getKey()).isEqualTo(123L);

    final RecordMetadata metadata = message.getMetadata();
    assertThat(metadata.getTopicName()).isEqualTo(StubBrokerRule.TEST_TOPIC_NAME);
    assertThat(metadata.getPartitionId()).isEqualTo(StubBrokerRule.TEST_PARTITION_ID);
    assertThat(metadata.getPosition()).isEqualTo(456);
    assertThat(metadata.getTimestamp()).isEqualTo(now);
    assertThat(metadata.getRejectionType()).isEqualTo(null);
    assertThat(metadata.getRejectionReason()).isEqualTo(null);
    assertThat(metadata.getSourceRecordPosition()).isEqualTo(1L);

    assertThat(message.getState()).isEqualTo(MessageState.PUBLISHED);
    assertThat(message.getMessageName()).isEqualTo("ORDER_CANCELLED");
    assertThat(message.getMessageKey()).isEqualTo("some-order-id");
    assertThat(message.getPayload()).isEqualTo(PAYLOAD);
  }
}
