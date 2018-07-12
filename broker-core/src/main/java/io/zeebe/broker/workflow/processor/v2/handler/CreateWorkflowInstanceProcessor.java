package io.zeebe.broker.workflow.processor.v2.handler;

import org.agrona.DirectBuffer;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedStreamProcessor;
import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.broker.workflow.map.DeployedWorkflow;
import io.zeebe.broker.workflow.map.WorkflowCache;
import io.zeebe.broker.workflow.processor.v2.RecordHandler;
import io.zeebe.broker.workflow.processor.v2.RecordWriter;
import io.zeebe.logstreams.processor.EventLifecycleContext;
import io.zeebe.model.bpmn.instance.StartEvent;
import io.zeebe.model.bpmn.instance.Workflow;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.impl.RecordMetadata;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.transport.ClientResponse;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;

public class CreateWorkflowInstanceProcessor implements RecordHandler<WorkflowInstanceRecord> {

  // TODO: why write both events here?
  private final WorkflowInstanceRecord startEventRecord = new WorkflowInstanceRecord();
  private final WorkflowCache workflowCache;

  private ActorControl actor;

  public CreateWorkflowInstanceProcessor(WorkflowCache workflowCache)
  {
    this.workflowCache = workflowCache;
  }

  @Override
  public void onOpen(TypedStreamProcessor streamProcessor) {
    actor = streamProcessor.getActor();
  }

  @Override
  public void handle(RecordWriter recordWriter, TypedRecord<WorkflowInstanceRecord> command,
      EventLifecycleContext ctx) {

    resolveWorkflowDefinition(recordWriter, command, ctx);
  }

  private void resolveWorkflowDefinition(
      RecordWriter recordWriter, TypedRecord<WorkflowInstanceRecord> command, EventLifecycleContext ctx) {
    final WorkflowInstanceRecord value = command.getValue();

    final long workflowKey = value.getWorkflowKey();
    final DirectBuffer bpmnProcessId = value.getBpmnProcessId();
    final int version = value.getVersion();

    ActorFuture<ClientResponse> fetchWorkflowFuture = null;

    if (workflowKey <= 0) {
      // by bpmn process id and version
      if (version > 0) {
        final DeployedWorkflow workflowDefinition =
            workflowCache.getWorkflowByProcessIdAndVersion(bpmnProcessId, version);

        if (workflowDefinition != null) {
          acceptCommand(command, workflowDefinition, recordWriter);
        } else {
          fetchWorkflowFuture =
              workflowCache.fetchWorkflowByBpmnProcessIdAndVersion(bpmnProcessId, version);
        }
      }

      // latest by bpmn process id
      else {
        final DeployedWorkflow workflowDefinition =
            workflowCache.getLatestWorkflowVersionByProcessId(bpmnProcessId);

        if (workflowDefinition != null && version != -2) {
          acceptCommand(command, workflowDefinition, recordWriter);
        } else {
          fetchWorkflowFuture = workflowCache.fetchLatestWorkflowByBpmnProcessId(bpmnProcessId);
        }
      }
    }

    // by key
    else {
      final DeployedWorkflow workflowDefinition = workflowCache.getWorkflowByKey(workflowKey);

      if (workflowDefinition != null) {
        acceptCommand(command, workflowDefinition, recordWriter);
      } else {
        fetchWorkflowFuture = workflowCache.fetchWorkflowByKey(workflowKey);
      }
    }

    if (fetchWorkflowFuture != null) {
      final ActorFuture<Void> workflowFetchedFuture = new CompletableActorFuture<>();
      ctx.async(workflowFetchedFuture);

      actor.runOnCompletion(
          fetchWorkflowFuture,
          (response, err) -> {
            if (err != null) {
              rejectCommand(
                  command,
                  recordWriter,
                  RejectionType.PROCESSING_ERROR,
                  "Could not fetch workflow: " + err.getMessage());
            } else {
              final DeployedWorkflow workflowDefinition =
                  workflowCache.addWorkflow(response.getResponseBuffer());

              if (workflowDefinition != null) {
                acceptCommand(command, workflowDefinition, recordWriter);
              } else {
                rejectCommand(
                    command,
                    recordWriter,
                    RejectionType.BAD_VALUE,
                    "Workflow is not deployed");
              }
            }

            workflowFetchedFuture.complete(null);
          });
    }
  }

  private void acceptCommand(
      TypedRecord<WorkflowInstanceRecord> command,
      DeployedWorkflow resolvedWorkflow,
      RecordWriter recordWriter)
  {
    final WorkflowInstanceRecord value = command.getValue();

    final long key = recordWriter.generateKey();
    value.setWorkflowInstanceKey(key);
    value.setWorkflowKey(resolvedWorkflow.getKey());
    value.setBpmnProcessId(resolvedWorkflow.getWorkflow().getBpmnProcessId());
    value.setVersion(resolvedWorkflow.getVersion());

    final RecordMetadata metadata = command.getMetadata();
    recordWriter.publishEvent(key, WorkflowInstanceIntent.CREATED, command.getValue(),
        m -> m.requestId(metadata.getRequestId()).requestStreamId(metadata.getRequestStreamId()));

    final Workflow workflow = resolvedWorkflow.getWorkflow();
    final StartEvent startEvent = workflow.getInitialStartEvent();
    final DirectBuffer activityId = startEvent.getIdAsBuffer();

    startEventRecord
        .setActivityId(activityId)
        .setBpmnProcessId(value.getBpmnProcessId())
        .setPayload(value.getPayload())
        .setVersion(value.getVersion())
        .setWorkflowInstanceKey(value.getWorkflowInstanceKey())
        .setWorkflowKey(value.getWorkflowKey());

    recordWriter.publishEvent(WorkflowInstanceIntent.START_EVENT_OCCURRED, startEventRecord);
  }

  private void rejectCommand(TypedRecord<WorkflowInstanceRecord> command, RecordWriter recordWriter, RejectionType type, String reason)
  {
    recordWriter.publishRejection(command, type, reason);
  }
}
