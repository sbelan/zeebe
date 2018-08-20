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
package io.zeebe.broker.it.workflow;

import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.entry;

import io.zeebe.broker.it.ClientRule;
import io.zeebe.broker.it.EmbeddedBrokerRule;
import io.zeebe.broker.it.util.TopicEventRecorder;
import io.zeebe.gateway.api.events.WorkflowInstanceEvent;
import io.zeebe.gateway.api.events.WorkflowInstanceState;
import io.zeebe.gateway.cmd.BrokerErrorException;
import io.zeebe.gateway.cmd.ClientCommandRejectedException;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import java.util.Collections;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

public class UpdatePayloadTest {
  private static final String PAYLOAD = "{\"foo\":\"bar\"}";

  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess("process")
          .startEvent("start")
          .serviceTask("task-1", t -> t.zeebeTaskType("task-1").zeebeOutput("$.result", "$.result"))
          .endEvent("end")
          .done();

  public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();
  public ClientRule clientRule = new ClientRule(brokerRule);
  public TopicEventRecorder eventRecorder = new TopicEventRecorder(clientRule);

  @Rule
  public RuleChain ruleChain =
      RuleChain.outerRule(brokerRule).around(clientRule).around(eventRecorder);

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before
  public void init() {
    clientRule
        .getWorkflowClient()
        .newDeployCommand()
        .addWorkflowModel(WORKFLOW, "workflow.bpmn")
        .send()
        .join();

    clientRule
        .getWorkflowClient()
        .newCreateInstanceCommand()
        .bpmnProcessId("process")
        .latestVersion()
        .send()
        .join();
  }

  @Test
  public void shouldUpdatePayloadWhenActivityIsActivated() {
    // given
    waitUntil(
        () -> eventRecorder.hasElementInState("task-1", WorkflowInstanceState.ELEMENT_ACTIVATED));

    final WorkflowInstanceEvent activtyInstance =
        eventRecorder.getElementInState("task-1", WorkflowInstanceState.ELEMENT_ACTIVATED);

    // when
    final WorkflowInstanceEvent payloadUpdated =
        clientRule
            .getWorkflowClient()
            .newUpdatePayloadCommand(activtyInstance)
            .payload(PAYLOAD)
            .send()
            .join();

    // then
    waitUntil(() -> eventRecorder.hasWorkflowInstanceEvent(WorkflowInstanceState.PAYLOAD_UPDATED));

    assertThat(payloadUpdated.getState()).isEqualTo(WorkflowInstanceState.PAYLOAD_UPDATED);
    assertThat(payloadUpdated.getPayload()).isEqualTo(PAYLOAD);
    assertThat(payloadUpdated.getPayloadAsMap()).containsOnly(entry("foo", "bar"));
  }

  @Test
  public void shouldUpdateWithNullPayload() {
    // given
    waitUntil(
        () -> eventRecorder.hasElementInState("task-1", WorkflowInstanceState.ELEMENT_ACTIVATED));

    final WorkflowInstanceEvent activtyInstance =
        eventRecorder.getElementInState("task-1", WorkflowInstanceState.ELEMENT_ACTIVATED);

    // when
    final WorkflowInstanceEvent payloadUpdated =
        clientRule
            .getWorkflowClient()
            .newUpdatePayloadCommand(activtyInstance)
            .payload("null")
            .send()
            .join();

    // then
    waitUntil(() -> eventRecorder.hasWorkflowInstanceEvent(WorkflowInstanceState.PAYLOAD_UPDATED));

    assertThat(payloadUpdated.getState()).isEqualTo(WorkflowInstanceState.PAYLOAD_UPDATED);
    assertThat(payloadUpdated.getPayload()).isEqualTo("{}");
    assertThat(payloadUpdated.getPayloadAsMap()).isEmpty();
  }

  @Test
  public void shouldThrowExceptionOnUpdateWithInvalidPayload() {
    // given
    waitUntil(
        () -> eventRecorder.hasElementInState("task-1", WorkflowInstanceState.ELEMENT_ACTIVATED));

    final WorkflowInstanceEvent activtyInstance =
        eventRecorder.getElementInState("task-1", WorkflowInstanceState.ELEMENT_ACTIVATED);

    // when
    final Throwable throwable =
        catchThrowable(
            () ->
                clientRule
                    .getWorkflowClient()
                    .newUpdatePayloadCommand(activtyInstance)
                    .payload("[]")
                    .send()
                    .join());

    // then
    assertThat(throwable).isInstanceOf(BrokerErrorException.class);
    assertThat(throwable.getMessage()).contains("Could not read property 'payload'.");
    assertThat(throwable.getMessage())
        .contains("Document has invalid format. On root level an object is only allowed.");
  }

  @Test
  public void shouldUpdatePayloadAndCompleteJobAfterwards() {
    // given
    waitUntil(
        () -> eventRecorder.hasElementInState("task-1", WorkflowInstanceState.ELEMENT_ACTIVATED));
    final WorkflowInstanceEvent activtyInstance =
        eventRecorder.getElementInState("task-1", WorkflowInstanceState.ELEMENT_ACTIVATED);

    clientRule
        .getWorkflowClient()
        .newUpdatePayloadCommand(activtyInstance)
        .payload(PAYLOAD)
        .send()
        .join();
    waitUntil(() -> eventRecorder.hasWorkflowInstanceEvent(WorkflowInstanceState.PAYLOAD_UPDATED));

    // when
    clientRule
        .getJobClient()
        .newWorker()
        .jobType("task-1")
        .handler(
            (client, job) -> client.newCompleteCommand(job).payload("{\"result\": \"ok\"}").send())
        .open();

    // then
    waitUntil(
        () -> eventRecorder.hasElementInState("process", WorkflowInstanceState.ELEMENT_COMPLETED));
    final WorkflowInstanceEvent wfEvent =
        eventRecorder.getElementInState("process", WorkflowInstanceState.ELEMENT_COMPLETED);
    assertThat(wfEvent.getPayload()).isEqualTo("{\"foo\":\"bar\",\"result\":\"ok\"}");
    assertThat(wfEvent.getPayloadAsMap())
        .hasSize(2)
        .contains(entry("foo", "bar"))
        .contains(entry("result", "ok"));
  }

  @Test
  public void shouldUpdatePayloadWithMap() {
    // given
    waitUntil(
        () -> eventRecorder.hasElementInState("task-1", WorkflowInstanceState.ELEMENT_ACTIVATED));

    final WorkflowInstanceEvent activtyInstance =
        eventRecorder.getElementInState("task-1", WorkflowInstanceState.ELEMENT_ACTIVATED);

    // when
    final WorkflowInstanceEvent event =
        clientRule
            .getWorkflowClient()
            .newUpdatePayloadCommand(activtyInstance)
            .payload(Collections.singletonMap("foo", "bar"))
            .send()
            .join();

    // then
    waitUntil(() -> eventRecorder.hasWorkflowInstanceEvent(WorkflowInstanceState.PAYLOAD_UPDATED));

    assertThat(event.getState()).isEqualTo(WorkflowInstanceState.PAYLOAD_UPDATED);
    assertThat(event.getPayload()).isEqualTo(PAYLOAD);
    assertThat(event.getPayloadAsMap()).containsOnly(entry("foo", "bar"));
  }

  @Test
  public void shouldUpdatePayloadWithObject() {
    // given
    final PayloadObject newPayload = new PayloadObject();
    newPayload.foo = "bar";

    waitUntil(
        () -> eventRecorder.hasElementInState("task-1", WorkflowInstanceState.ELEMENT_ACTIVATED));

    final WorkflowInstanceEvent activtyInstance =
        eventRecorder.getElementInState("task-1", WorkflowInstanceState.ELEMENT_ACTIVATED);

    // when
    final WorkflowInstanceEvent event =
        clientRule
            .getWorkflowClient()
            .newUpdatePayloadCommand(activtyInstance)
            .payload(newPayload)
            .send()
            .join();

    // then
    waitUntil(() -> eventRecorder.hasWorkflowInstanceEvent(WorkflowInstanceState.PAYLOAD_UPDATED));

    assertThat(event.getState()).isEqualTo(WorkflowInstanceState.PAYLOAD_UPDATED);
    assertThat(event.getPayload()).isEqualTo(PAYLOAD);
    assertThat(event.getPayloadAsMap()).containsOnly(entry("foo", "bar"));
  }

  @Test
  public void shouldFailUpdatePayloadIfWorkflowInstanceIsCompleted() {
    // given
    clientRule
        .getJobClient()
        .newWorker()
        .jobType("task-1")
        .handler(
            (client, job) ->
                client.newCompleteCommand(job).payload("{\"result\": \"done\"}").send())
        .open();

    waitUntil(
        () -> eventRecorder.hasElementInState("process", WorkflowInstanceState.ELEMENT_COMPLETED));

    final WorkflowInstanceEvent activityInstance =
        eventRecorder.getElementInState("task-1", WorkflowInstanceState.ELEMENT_ACTIVATED);

    // then
    thrown.expect(ClientCommandRejectedException.class);
    thrown.expectMessage(
        "Command (UPDATE_PAYLOAD) for event with key "
            + activityInstance.getMetadata().getKey()
            + " was rejected");

    // when
    clientRule
        .getWorkflowClient()
        .newUpdatePayloadCommand(activityInstance)
        .payload(PAYLOAD)
        .send()
        .join();
  }

  public static class PayloadObject {
    public String foo;
  }
}
