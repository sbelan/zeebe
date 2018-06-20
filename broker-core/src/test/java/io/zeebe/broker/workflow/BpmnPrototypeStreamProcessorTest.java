package io.zeebe.broker.workflow;

import static io.zeebe.test.util.TestUtil.doRepeatedly;
import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.agrona.DirectBuffer;
import org.agrona.io.DirectBufferInputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.zeebe.broker.job.data.JobRecord;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.topic.StreamProcessorControl;
import io.zeebe.broker.util.StreamProcessorRule;
import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.broker.workflow.map.DeployedWorkflow;
import io.zeebe.broker.workflow.map.WorkflowCache;
import io.zeebe.broker.workflow.processor.WorkflowInstanceStreamProcessor;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.instance.Workflow;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.impl.RecordMetadata;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.test.util.MsgPackUtil;
import io.zeebe.util.buffer.BufferUtil;

public class BpmnPrototypeStreamProcessorTest {

  public static final ObjectMapper MSGPACK_MAPPER = new ObjectMapper(new MessagePackFactory());
  private static final long WORKFLOW_KEY = 1;

  private static final Workflow FORK_JOIN_FLOW = Bpmn.createExecutableWorkflow("foo")
      .startEvent()
      .parallelGateway("fork")
      .serviceTask("foo", t -> t.taskType("foo"))
      .parallelGateway("join")
      .endEvent()
      .continueAt("fork")
      .serviceTask("bar", t -> t.taskType("bar"))
      .connectTo("join")
      .done()
      .getWorkflow(BufferUtil.wrapString("foo"));

  @Rule
  public StreamProcessorRule rule = new StreamProcessorRule();
  private WorkflowCache workflowCache;
  private WorkflowInstanceStreamProcessor processor;

  private StreamProcessorControl streamProcessorControl;

  @Before
  public void setUp() {
    workflowCache = mock(WorkflowCache.class);

    processor = new WorkflowInstanceStreamProcessor(workflowCache, 1024 * 32);

    streamProcessorControl = rule.runStreamProcessor(processor::createStreamProcessor);
  }

  @After
  public void tearDown()
  {

    // TODO: this could go as a configurable option into the test rule
    final List<TypedRecord<UnpackedObject>> records = rule.events()
        .asTypedRecords()
        .collect(Collectors.toList());

    final StringBuilder sb = new StringBuilder();
    sb.append("Record stream (");
    sb.append(records.size());
    sb.append(" records):\n");
    for (TypedRecord<UnpackedObject> record : records)
    {
      final RecordMetadata metadata = record.getMetadata();
      sb.append(metadata.getRecordType());
      sb.append(", ");
      sb.append(metadata.getValueType());
      sb.append(", ");
      sb.append(metadata.getIntent());
      sb.append("\n");
    }

    System.out.println(sb.toString());
  }

  @Test
  public void shouldSplitOnParallelGateway() {
    // given
    deploy(WORKFLOW_KEY, FORK_JOIN_FLOW);

    // when
    rule.writeCommand(WorkflowInstanceIntent.CREATE, startWorkflowInstance(WORKFLOW_KEY));

    // then
    waitUntil(() -> rule.events().onlyWorkflowInstanceRecords()
        .withIntent(WorkflowInstanceIntent.ACTIVITY_ACTIVATED).count() == 2);
  }

  @Test
  public void shouldMergeOnParallelGateway() {
    // given
    deploy(WORKFLOW_KEY, FORK_JOIN_FLOW);
    rule.writeCommand(WorkflowInstanceIntent.CREATE, startWorkflowInstance(WORKFLOW_KEY));

    final List<TypedRecord<JobRecord>> jobCommands =
        doRepeatedly(() -> rule.events().onlyJobRecords().withIntent(JobIntent.CREATE)
            .collect(Collectors.toList())).until(c -> c.size() == 2);

    // when
    for (TypedRecord<JobRecord> createCommand : jobCommands)
    {
      rule.writeEvent(createCommand.getKey(), JobIntent.CREATED, createCommand.getValue()); // => required for workflow stream processor indexing
      rule.writeEvent(createCommand.getKey(), JobIntent.COMPLETED, createCommand.getValue());
    }

    // then
    waitUntil(() -> rule.events().onlyWorkflowInstanceRecords()
        .withIntent(WorkflowInstanceIntent.END_EVENT_OCCURRED).findFirst().isPresent());

    final List<TypedRecord<WorkflowInstanceRecord>> workflowInstanceEvents = rule.events()
        .onlyWorkflowInstanceRecords()
        .onlyEvents()
        .collect(Collectors.toList());


    assertThat(workflowInstanceEvents).extracting(e -> e.getMetadata().getIntent())
    .containsExactly(
        WorkflowInstanceIntent.CREATED,
        WorkflowInstanceIntent.START_EVENT_OCCURRED,
        WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN,
        WorkflowInstanceIntent.GATEWAY_ACTIVATED,
        WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN,
        WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN,
        WorkflowInstanceIntent.ACTIVITY_READY,
        WorkflowInstanceIntent.ACTIVITY_READY,
        WorkflowInstanceIntent.ACTIVITY_ACTIVATED,
        WorkflowInstanceIntent.ACTIVITY_ACTIVATED,
        // TODO: to assert concurrent execution correctly, we would need subsequence matching whe
        // each element is matched at most once, which assertj doesn't provide out of the box
        WorkflowInstanceIntent.ACTIVITY_COMPLETING,
        WorkflowInstanceIntent.ACTIVITY_COMPLETING,
        WorkflowInstanceIntent.ACTIVITY_COMPLETED,
        WorkflowInstanceIntent.ACTIVITY_COMPLETED,
        WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN,
        WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN,
        WorkflowInstanceIntent.GATEWAY_ACTIVATED,
        WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN,
        WorkflowInstanceIntent.END_EVENT_OCCURRED,
        WorkflowInstanceIntent.COMPLETED);

  }

  @Test
  public void shouldPropagatePayloadOnSplit()
  {
    // given
    deploy(WORKFLOW_KEY, FORK_JOIN_FLOW);
    final WorkflowInstanceRecord startCommand = startWorkflowInstance(WORKFLOW_KEY);
    startCommand.setPayload(MsgPackUtil.asMsgPack("foo", "bar"));

    // when
    rule.writeCommand(WorkflowInstanceIntent.CREATE, startCommand);

    // then
    final List<TypedRecord<JobRecord>> jobCommands =
        doRepeatedly(() -> rule.events().onlyJobRecords().withIntent(JobIntent.CREATE)
            .collect(Collectors.toList())).until(c -> c.size() == 2);

    assertThat(jobCommands).extracting(c -> c.getValue().getPayload())
      .allMatch(payload -> payload.equals(startCommand.getPayload()));
  }

  @Test
  public void shouldSynchronizeProcessCompletion() {
    fail("implement; build when scopes are in place as we have the same problem there");
  }

  @Test
  public void shouldMergePayloadsOnParallelMerge()
  {
    // given
    deploy(WORKFLOW_KEY, FORK_JOIN_FLOW);
    rule.writeCommand(WorkflowInstanceIntent.CREATE, startWorkflowInstance(WORKFLOW_KEY));

    final List<TypedRecord<JobRecord>> jobCommands =
        doRepeatedly(() -> rule.events().onlyJobRecords().withIntent(JobIntent.CREATE)
            .collect(Collectors.toList())).until(c -> c.size() == 2);

    for (TypedRecord<JobRecord> createCommand : jobCommands)
    {
      rule.writeEvent(createCommand.getKey(), JobIntent.CREATED, createCommand.getValue()); // => required for workflow stream processor indexing
    }

    final TypedRecord<JobRecord> job1 = jobCommands.get(0);
    job1.getValue().setPayload(MsgPackUtil.asMsgPack("key1", "val1"));

    final TypedRecord<JobRecord> job2 = jobCommands.get(1);
    job2.getValue().setPayload(MsgPackUtil.asMsgPack("key2", "val2"));

    // when
    rule.writeEvent(job1.getKey(), JobIntent.COMPLETED, job1.getValue());
    rule.writeEvent(job2.getKey(), JobIntent.COMPLETED, job2.getValue());

    // then
    final TypedRecord<WorkflowInstanceRecord> endEvent = doRepeatedly(() -> rule.events().onlyWorkflowInstanceRecords()
        .withIntent(WorkflowInstanceIntent.END_EVENT_OCCURRED).findFirst()).until(e -> e.isPresent()).get();

    final DirectBuffer mergedPayload = endEvent.getValue().getPayload();
    assertThat(msgPackAsMap(mergedPayload))
      .containsExactly(entry("key1", "val1"), entry("key2", "val2"));
  }

  /**
   * XML:
   *
   * <pre>
   * <bpmn:parallelGateway id="gw">
   *  <extensionElements>
   *    <zeebe:mergeInstruction operation="PUT" element="flow1" ="$.foo" target="$.bar"/>
   *    <zeebe:mergeInstruction operation="REMOVE" target="$.foo"/>
   *  </extensionElements>
   *  <bpmn:incoming>flow1</bpmn:incoming>
   *  <bpmn:incoming>flow2</bpmn:incoming>
   *</bpmn:parallelGateway>
   *</pre>
   */
  @Test
  public void shouldMergePayloadsAndApplyMappingsOnParallelMerge()
  {
    // given
    final Workflow workflow = Bpmn.createExecutableWorkflow("foo")
        .startEvent()
        .parallelGateway("fork")
        .serviceTask("foo", t -> t.taskType("foo"))
        .sequenceFlow("joinFlow1")
        .parallelGateway("join", b ->
          b.mergeInstructionPut("joinFlow1", "$.key1", "$.key3")
            .mergeInstructionRemove("$.key1"))
        .endEvent()
        .continueAt("fork")
        .serviceTask("bar", t -> t.taskType("bar"))
        .sequenceFlow("joinFlow2")
        .connectTo("join")
        .done()
        .getWorkflow(BufferUtil.wrapString("foo"));
    deploy(WORKFLOW_KEY, workflow);
    rule.writeCommand(WorkflowInstanceIntent.CREATE, startWorkflowInstance(WORKFLOW_KEY));

    final List<TypedRecord<JobRecord>> jobCommands =
        doRepeatedly(() -> rule.events().onlyJobRecords().withIntent(JobIntent.CREATE)
            .collect(Collectors.toList())).until(c -> c.size() == 2);

    for (TypedRecord<JobRecord> createCommand : jobCommands)
    {
      rule.writeEvent(createCommand.getKey(), JobIntent.CREATED, createCommand.getValue()); // => required for workflow stream processor indexing
    }

    final TypedRecord<JobRecord> job1 = jobCommands.get(0);
    job1.getValue().setPayload(MsgPackUtil.asMsgPack("key1", "val1"));

    final TypedRecord<JobRecord> job2 = jobCommands.get(1);
    job2.getValue().setPayload(MsgPackUtil.asMsgPack("key2", "val2"));

    // when
    rule.writeEvent(job1.getKey(), JobIntent.COMPLETED, job1.getValue());
    rule.writeEvent(job2.getKey(), JobIntent.COMPLETED, job2.getValue());

    // then
    final TypedRecord<WorkflowInstanceRecord> endEvent = doRepeatedly(() -> rule.events().onlyWorkflowInstanceRecords()
        .withIntent(WorkflowInstanceIntent.END_EVENT_OCCURRED).findFirst()).until(e -> e.isPresent()).get();

    final DirectBuffer mergedPayload = endEvent.getValue().getPayload();
    fail("instructions are not implemented yet");
    assertThat(msgPackAsMap(mergedPayload))
      .containsExactly(entry("key3", "val1"), entry("key2", "val2"));

  }

  @Test
  public void shouldForkJoinInOneGateway()
  {
    fail("implement");
  }

  @Test
  public void shouldMergePayloadsOnScopeCompletion()
  {
    fail("implement");

  }

  @Test
  public void shouldEnterEmbeddedSubprocess()
  {
    fail("implement");
  }

  private void deploy(long key, Workflow workflow) {
    when(workflowCache.getWorkflowByKey(key)).thenReturn(new DeployedWorkflow(workflow, key, 1, 1));
  }

  private static WorkflowInstanceRecord startWorkflowInstance(long key) {
    final WorkflowInstanceRecord record = new WorkflowInstanceRecord();
    record.setWorkflowKey(key);
    return record;
  }

  public static Map<String, Object> msgPackAsMap(DirectBuffer msgPack)
  {

    try {
      return MSGPACK_MAPPER.readValue(new DirectBufferInputStream(msgPack), Map.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
