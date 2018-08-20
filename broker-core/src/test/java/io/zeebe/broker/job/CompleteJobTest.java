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
package io.zeebe.broker.job;

import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.entry;

import io.zeebe.broker.test.EmbeddedBrokerRule;
import io.zeebe.broker.test.MsgPackUtil;
import io.zeebe.msgpack.spec.MsgPackHelper;
import io.zeebe.protocol.clientapi.ControlMessageType;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.test.broker.protocol.clientapi.ClientApiRule;
import io.zeebe.test.broker.protocol.clientapi.ExecuteCommandResponse;
import io.zeebe.test.broker.protocol.clientapi.SubscribedRecord;
import io.zeebe.test.broker.protocol.clientapi.TestTopicClient;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public class CompleteJobTest {
  private static final String JOB_TYPE = "foo";

  public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();
  public ClientApiRule apiRule = new ClientApiRule(brokerRule::getClientAddress);

  private TestTopicClient testClient;

  @Rule public RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(apiRule);

  @Before
  public void setUp() {
    testClient = apiRule.topic();
  }

  @Test
  public void shouldCompleteJob() {
    // given
    createJob(JOB_TYPE);

    apiRule.openJobSubscription(JOB_TYPE).await();

    final SubscribedRecord subscribedEvent = receiveSingleSubscribedEvent();

    // when
    final ExecuteCommandResponse response =
        completeJob(subscribedEvent.key(), subscribedEvent.value());

    // then
    final SubscribedRecord completeEvent = testClient.receiveFirstJobCommand(JobIntent.COMPLETE);

    assertThat(response.recordType()).isEqualTo(RecordType.EVENT);
    assertThat(response.sourceRecordPosition()).isEqualTo(completeEvent.position());
    assertThat(response.intent()).isEqualTo(JobIntent.COMPLETED);
  }

  @Test
  public void shouldRejectCompletionIfJobNotFound() {
    // given
    final int key = 123;

    final Map<String, Object> event = new HashMap<>();
    event.put("type", "foo");

    // when
    final ExecuteCommandResponse response = completeJob(key, event);

    // then
    final SubscribedRecord completeEvent = testClient.receiveFirstJobCommand(JobIntent.COMPLETE);

    assertThat(response.recordType()).isEqualTo(RecordType.COMMAND_REJECTION);
    assertThat(response.intent()).isEqualTo(JobIntent.COMPLETE);
    assertThat(response.sourceRecordPosition()).isEqualTo(completeEvent.position());
    assertThat(response.rejectionType()).isEqualTo(RejectionType.NOT_APPLICABLE);
    assertThat(response.rejectionReason()).isEqualTo("Job is not in state: ACTIVATED, TIMED_OUT");
  }

  @Test
  public void shouldCompleteJobWithPayload() {
    // given
    createJob(JOB_TYPE);

    apiRule.openJobSubscription(JOB_TYPE).await();

    final SubscribedRecord subscribedEvent = receiveSingleSubscribedEvent();

    // when
    subscribedEvent.value().put("payload", MsgPackUtil.MSGPACK_PAYLOAD);
    final ExecuteCommandResponse response =
        completeJob(subscribedEvent.key(), subscribedEvent.value());

    // then
    assertThat(response.recordType()).isEqualTo(RecordType.EVENT);
    assertThat(response.intent()).isEqualTo(JobIntent.COMPLETED);
    assertThat(response.getValue()).contains(entry("payload", MsgPackUtil.MSGPACK_PAYLOAD));
  }

  @Test
  public void shouldCompleteJobWithNilPayload() {
    // given
    createJob(JOB_TYPE);

    apiRule.openJobSubscription(JOB_TYPE).await();

    final SubscribedRecord subscribedEvent = receiveSingleSubscribedEvent();

    // when
    subscribedEvent.value().put("payload", MsgPackHelper.NIL);
    final ExecuteCommandResponse response =
        completeJob(subscribedEvent.key(), subscribedEvent.value());

    // then
    assertThat(response.recordType()).isEqualTo(RecordType.EVENT);
    assertThat(response.intent()).isEqualTo(JobIntent.COMPLETED);
    assertThat(response.getValue()).contains(entry("payload", MsgPackHelper.EMTPY_OBJECT));
  }

  @Test
  public void shouldCompleteJobWithZeroLengthPayload() {
    // given
    createJob(JOB_TYPE);

    apiRule.openJobSubscription(JOB_TYPE).await();

    final SubscribedRecord subscribedEvent = receiveSingleSubscribedEvent();

    // when
    subscribedEvent.value().put("payload", new byte[0]);
    final ExecuteCommandResponse response =
        completeJob(subscribedEvent.key(), subscribedEvent.value());

    // then
    assertThat(response.recordType()).isEqualTo(RecordType.EVENT);
    assertThat(response.intent()).isEqualTo(JobIntent.COMPLETED);
    assertThat(response.getValue()).contains(entry("payload", MsgPackHelper.EMTPY_OBJECT));
  }

  @Test
  public void shouldCompleteJobWithNoPayload() {
    // given
    createJob(JOB_TYPE);

    apiRule.openJobSubscription(JOB_TYPE).await();

    final SubscribedRecord subscribedEvent = receiveSingleSubscribedEvent();

    // when
    final ExecuteCommandResponse response =
        completeJob(subscribedEvent.key(), subscribedEvent.value());

    // then
    assertThat(response.recordType()).isEqualTo(RecordType.EVENT);
    assertThat(response.intent()).isEqualTo(JobIntent.COMPLETED);
    assertThat(response.getValue()).contains(entry("payload", MsgPackHelper.EMTPY_OBJECT));
  }

  @Test
  public void shouldThrowExceptionOnCompletionIfPayloadIsInvalid() {
    // given
    createJob(JOB_TYPE);

    apiRule.openJobSubscription(JOB_TYPE).await();

    final SubscribedRecord subscribedEvent = receiveSingleSubscribedEvent();

    final Map<String, Object> event = subscribedEvent.value();
    event.put("payload", new byte[] {1}); // positive fixnum, i.e. no object

    // when
    final Throwable throwable = catchThrowable(() -> completeJob(subscribedEvent.key(), event));

    // then
    assertThat(throwable).isInstanceOf(RuntimeException.class);
    assertThat(throwable.getMessage()).contains("Could not read property 'payload'.");
    assertThat(throwable.getMessage())
        .contains("Document has invalid format. On root level an object is only allowed.");
  }

  @Test
  public void shouldRejectCompletionIfJobIsCompleted() {
    // given
    final ExecuteCommandResponse response2 = createJob(JOB_TYPE);
    assertThat(response2.recordType()).isEqualTo(RecordType.EVENT);

    apiRule.openJobSubscription(JOB_TYPE).await();

    final SubscribedRecord subscribedEvent = receiveSingleSubscribedEvent();
    completeJob(subscribedEvent.key(), subscribedEvent.value());

    // when
    final ExecuteCommandResponse response =
        completeJob(subscribedEvent.key(), subscribedEvent.value());

    // then
    assertThat(response.recordType()).isEqualTo(RecordType.COMMAND_REJECTION);
    assertThat(response.rejectionType()).isEqualTo(RejectionType.NOT_APPLICABLE);
    assertThat(response.rejectionReason()).isEqualTo("Job is not in state: ACTIVATED, TIMED_OUT");
    assertThat(response.intent()).isEqualTo(JobIntent.COMPLETE);
  }

  @Test
  public void shouldRejectCompletionIfJobNotActivated() {
    // given
    final ExecuteCommandResponse job = createJob(JOB_TYPE);

    // when
    final ExecuteCommandResponse response = completeJob(job.key(), job.getValue());

    // then
    assertThat(response.recordType()).isEqualTo(RecordType.COMMAND_REJECTION);
    assertThat(response.rejectionType()).isEqualTo(RejectionType.NOT_APPLICABLE);
    assertThat(response.rejectionReason()).isEqualTo("Job is not in state: ACTIVATED, TIMED_OUT");
    assertThat(response.intent()).isEqualTo(JobIntent.COMPLETE);
  }

  @Test
  public void shouldCompleteIfNotWorker() {
    // given
    final String worker = "kermit";

    createJob(JOB_TYPE);

    apiRule
        .createControlMessageRequest()
        .partitionId(apiRule.getDefaultPartitionId())
        .messageType(ControlMessageType.ADD_JOB_SUBSCRIPTION)
        .data()
        .put("jobType", JOB_TYPE)
        .put("timeout", Duration.ofSeconds(30).toMillis())
        .put("worker", worker)
        .put("credits", 10)
        .done()
        .sendAndAwait();

    final SubscribedRecord subscribedEvent = receiveSingleSubscribedEvent();
    final Map<String, Object> event = subscribedEvent.value();
    event.put("worker", "ms piggy");

    // when
    final ExecuteCommandResponse response = completeJob(subscribedEvent.key(), event);

    // then
    final SubscribedRecord jobCommand = testClient.receiveFirstJobCommand(JobIntent.COMPLETE);

    assertThat(response.recordType()).isEqualTo(RecordType.EVENT);
    assertThat(response.sourceRecordPosition()).isEqualTo(jobCommand.position());
    assertThat(response.intent()).isEqualTo(JobIntent.COMPLETED);
  }

  private ExecuteCommandResponse createJob(String type) {
    return apiRule
        .createCmdRequest()
        .type(ValueType.JOB, JobIntent.CREATE)
        .command()
        .put("type", type)
        .put("retries", 3)
        .done()
        .sendAndAwait();
  }

  private ExecuteCommandResponse completeJob(long key, Map<String, Object> event) {
    return apiRule
        .createCmdRequest()
        .type(ValueType.JOB, JobIntent.COMPLETE)
        .key(key)
        .command()
        .putAll(event)
        .done()
        .sendAndAwait();
  }

  private SubscribedRecord receiveSingleSubscribedEvent() {
    waitUntil(() -> apiRule.numSubscribedEventsAvailable() == 1);
    return apiRule.subscribedEvents().findFirst().get();
  }
}
