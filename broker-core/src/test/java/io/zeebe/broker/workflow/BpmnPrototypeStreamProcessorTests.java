package io.zeebe.broker.workflow;

import static io.zeebe.test.util.TestUtil.doRepeatedly;
import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import io.zeebe.broker.job.data.JobRecord;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.topic.StreamProcessorControl;
import io.zeebe.broker.util.StreamProcessorRule;
import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.broker.workflow.map.DeployedWorkflow;
import io.zeebe.broker.workflow.map.WorkflowCache;
import io.zeebe.broker.workflow.processor.WorkflowInstanceStreamProcessor;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.instance.Workflow;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.impl.RecordMetadata;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.util.buffer.BufferUtil;

public class BpmnPrototypeStreamProcessorTests {

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

    final List<TypedRecord<WorkflowInstanceRecord>> workflowInstanceRecords = rule.events()
        .onlyWorkflowInstanceRecords()
        .onlyEvents()
        .collect(Collectors.toList());

    System.out.println(workflowInstanceRecords);

    fail("assert");

  }

  @Test
  public void shouldSynchronizeProcessCompletion() {
    fail("implement");
  }

  @Test
  public void shouldMergePayloadsOnParallelMerge()
  {
    fail("implement");

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

  private void deploy(long key, Workflow workflow) {
    when(workflowCache.getWorkflowByKey(key)).thenReturn(new DeployedWorkflow(workflow, key, 1, 1));
  }

  private static WorkflowInstanceRecord startWorkflowInstance(long key) {
    final WorkflowInstanceRecord record = new WorkflowInstanceRecord();
    record.setWorkflowKey(key);
    return record;
  }
}
