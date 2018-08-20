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
package io.zeebe.gateway.workflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import io.zeebe.gateway.api.clients.WorkflowClient;
import io.zeebe.gateway.api.events.WorkflowInstanceEvent;
import io.zeebe.gateway.api.events.WorkflowInstanceState;
import io.zeebe.gateway.cmd.ClientCommandRejectedException;
import io.zeebe.gateway.cmd.ClientException;
import io.zeebe.gateway.impl.data.MsgPackConverter;
import io.zeebe.gateway.impl.event.WorkflowInstanceEventImpl;
import io.zeebe.gateway.util.ClientRule;
import io.zeebe.gateway.util.Events;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.test.broker.protocol.brokerapi.ExecuteCommandRequest;
import io.zeebe.test.broker.protocol.brokerapi.StubBrokerRule;
import java.util.Collections;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

public class UpdatePayloadTest {
  private static final String PAYLOAD = "{\"foo\":\"bar\"}";

  private static final byte[] ENCODED_PAYLOAD = new MsgPackConverter().convertToMsgPack(PAYLOAD);

  public StubBrokerRule brokerRule = new StubBrokerRule();
  public ClientRule clientRule = new ClientRule(brokerRule);

  @Rule public RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(clientRule);

  @Rule public ExpectedException thrown = ExpectedException.none();

  private WorkflowClient workflowTopicClient;

  @Before
  public void setUp() {
    this.workflowTopicClient = clientRule.getClient().topicClient().workflowClient();

    brokerRule.workflowInstances().registerUpdatedPayloadCommand();
  }

  @Test
  public void shouldUpdatePayload() {
    // given
    final WorkflowInstanceEventImpl baseEvent = Events.exampleWorfklowInstance();
    baseEvent.setPosition(3L);
    baseEvent.setKey(2L);
    baseEvent.setSourceRecordPosition(1L);
    baseEvent.setWorkflowInstanceKey(1L);

    brokerRule.workflowInstances().registerUpdatedPayloadCommand(b -> b.sourceRecordPosition(4L));

    // when
    final WorkflowInstanceEvent workflowInstanceEvent =
        workflowTopicClient.newUpdatePayloadCommand(baseEvent).payload(PAYLOAD).send().join();

    // then
    assertThat(brokerRule.getReceivedCommandRequests()).hasSize(1);

    final ExecuteCommandRequest request = brokerRule.getReceivedCommandRequests().get(0);
    assertThat(request.valueType()).isEqualTo(ValueType.WORKFLOW_INSTANCE);
    assertThat(request.intent()).isEqualTo(WorkflowInstanceIntent.UPDATE_PAYLOAD);
    assertThat(request.sourceRecordPosition()).isEqualTo(3L);
    assertThat(request.key()).isEqualTo(2L);
    assertThat(request.partitionId()).isEqualTo(baseEvent.getMetadata().getPartitionId());
    assertThat(request.position()).isEqualTo(baseEvent.getMetadata().getPosition());

    assertThat(request.getCommand())
        .containsOnly(
            entry("bpmnProcessId", baseEvent.getBpmnProcessId()),
            entry("version", (long) baseEvent.getVersion()),
            entry("workflowKey", baseEvent.getWorkflowKey()),
            entry("workflowInstanceKey", baseEvent.getWorkflowInstanceKey()),
            entry("activityId", baseEvent.getActivityId()),
            entry("scopeInstanceKey", baseEvent.getScopeInstanceKey()),
            entry("payload", ENCODED_PAYLOAD));

    assertThat(workflowInstanceEvent.getMetadata().getSourceRecordPosition()).isEqualTo(4L);
    assertThat(workflowInstanceEvent.getState()).isEqualTo(WorkflowInstanceState.PAYLOAD_UPDATED);
  }

  @Test
  public void shouldUpdatePayloadAsMap() {
    // given
    final WorkflowInstanceEventImpl baseEvent = Events.exampleWorfklowInstance();

    // when
    workflowTopicClient
        .newUpdatePayloadCommand(baseEvent)
        .payload(Collections.singletonMap("foo", "bar"))
        .send()
        .join();

    // then
    assertThat(brokerRule.getReceivedCommandRequests()).hasSize(1);

    final ExecuteCommandRequest request = brokerRule.getReceivedCommandRequests().get(0);
    assertThat(request.getCommand()).contains(entry("payload", ENCODED_PAYLOAD));
  }

  @Test
  public void shouldUpdatePayloadAsObject() {
    // given
    final WorkflowInstanceEventImpl baseEvent = Events.exampleWorfklowInstance();

    final PayloadObject payload = new PayloadObject();
    payload.foo = "bar";

    // when
    workflowTopicClient.newUpdatePayloadCommand(baseEvent).payload(payload).send().join();

    // then
    assertThat(brokerRule.getReceivedCommandRequests()).hasSize(1);

    final ExecuteCommandRequest request = brokerRule.getReceivedCommandRequests().get(0);
    assertThat(request.getCommand()).contains(entry("payload", ENCODED_PAYLOAD));
  }

  @Test
  public void shouldThrowExceptionOnRejection() {
    // given
    final WorkflowInstanceEventImpl event = Events.exampleWorfklowInstance();
    event.setKey(2L);
    event.setWorkflowInstanceKey(1L);

    brokerRule.workflowInstances().registerUpdatedPayloadCommand(r -> r.rejection());

    // then
    thrown.expect(ClientCommandRejectedException.class);
    thrown.expectMessage("Command (UPDATE_PAYLOAD) for event with key 2 was rejected");

    // when
    workflowTopicClient.newUpdatePayloadCommand(event).payload(PAYLOAD).send().join();
  }

  @Test
  public void shouldThrowExceptionIfBaseEventIsNull() {
    // then
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("base event must not be null");

    // when
    workflowTopicClient.newUpdatePayloadCommand(null).payload(PAYLOAD).send();
  }

  @Test
  public void shouldThrowExceptionIfFailedToSerializePayload() {
    // given
    final WorkflowInstanceEventImpl baseEvent = Events.exampleWorfklowInstance();

    class NotSerializable {}

    // then
    thrown.expect(ClientException.class);
    thrown.expectMessage("Failed to serialize object");

    // when
    workflowTopicClient
        .newUpdatePayloadCommand(baseEvent)
        .payload(new NotSerializable())
        .send()
        .join();
  }

  public static class PayloadObject {
    public String foo;
  }
}
