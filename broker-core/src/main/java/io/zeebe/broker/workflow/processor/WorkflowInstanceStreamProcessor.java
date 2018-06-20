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
package io.zeebe.broker.workflow.processor;

import static io.zeebe.broker.workflow.data.WorkflowInstanceRecord.EMPTY_PAYLOAD;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import io.zeebe.broker.incident.data.ErrorType;
import io.zeebe.broker.incident.data.IncidentRecord;
import io.zeebe.broker.job.data.JobHeaders;
import io.zeebe.broker.job.data.JobRecord;
import io.zeebe.broker.logstreams.processor.CommandProcessor;
import io.zeebe.broker.logstreams.processor.StreamProcessorLifecycleAware;
import io.zeebe.broker.logstreams.processor.TypedBatchWriter;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.logstreams.processor.TypedStreamProcessor;
import io.zeebe.broker.logstreams.processor.TypedStreamReader;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.broker.workflow.map.ActivityInstanceMap;
import io.zeebe.broker.workflow.map.DeployedWorkflow;
import io.zeebe.broker.workflow.map.PayloadCache;
import io.zeebe.broker.workflow.map.WorkflowCache;
import io.zeebe.broker.workflow.map.WorkflowInstanceIndex;
import io.zeebe.broker.workflow.map.WorkflowInstanceIndex.OldWorkflowInstance;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.processor.EventLifecycleContext;
import io.zeebe.logstreams.processor.StreamProcessorContext;
import io.zeebe.model.bpmn.BpmnAspect;
import io.zeebe.model.bpmn.impl.instance.ServiceTaskImpl;
import io.zeebe.model.bpmn.impl.instance.SubProcessImpl;
import io.zeebe.model.bpmn.instance.EndEvent;
import io.zeebe.model.bpmn.instance.ExclusiveGateway;
import io.zeebe.model.bpmn.instance.FlowElement;
import io.zeebe.model.bpmn.instance.FlowNode;
import io.zeebe.model.bpmn.instance.InputOutputMapping;
import io.zeebe.model.bpmn.instance.OutputBehavior;
import io.zeebe.model.bpmn.instance.SequenceFlow;
import io.zeebe.model.bpmn.instance.ServiceTask;
import io.zeebe.model.bpmn.instance.StartEvent;
import io.zeebe.model.bpmn.instance.TaskDefinition;
import io.zeebe.model.bpmn.instance.Workflow;
import io.zeebe.msgpack.el.CompiledJsonCondition;
import io.zeebe.msgpack.el.JsonConditionException;
import io.zeebe.msgpack.el.JsonConditionInterpreter;
import io.zeebe.msgpack.mapping.Mapping;
import io.zeebe.msgpack.mapping.MappingException;
import io.zeebe.msgpack.mapping.MappingProcessor;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.RecordMetadata;
import io.zeebe.protocol.intent.IncidentIntent;
import io.zeebe.protocol.intent.Intent;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.transport.ClientResponse;
import io.zeebe.util.metrics.Metric;
import io.zeebe.util.metrics.MetricsManager;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;

public class WorkflowInstanceStreamProcessor implements StreamProcessorLifecycleAware {
  private static final UnsafeBuffer EMPTY_JOB_TYPE = new UnsafeBuffer("".getBytes());

  private Metric workflowInstanceEventCreate;
  private Metric workflowInstanceEventCanceled;
  private Metric workflowInstanceEventCompleted;

  private final PayloadCache payloadCache;

  private final WorkflowInstances workflowInstances = new WorkflowInstances();

  private final MappingProcessor payloadMappingProcessor = new MappingProcessor(4096);
  private final JsonConditionInterpreter conditionInterpreter = new JsonConditionInterpreter();

  private final WorkflowCache workflowCache;

  private ActorControl actor;

  public WorkflowInstanceStreamProcessor(WorkflowCache workflowCache, int payloadCacheSize) {
    this.workflowCache = workflowCache;
    this.payloadCache = new PayloadCache(payloadCacheSize);
  }

  public TypedStreamProcessor createStreamProcessor(TypedStreamEnvironment environment) {
    final BpmnAspectEventProcessor bpmnAspectProcessor = new BpmnAspectEventProcessor();

    return environment
        .newStreamProcessor()
        .onCommand(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.CREATE,
            new CreateWorkflowInstanceEventProcessor())
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.CREATED,
            new WorkflowInstanceCreatedEventProcessor())
        .onRejection(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.CREATE,
            new WorkflowInstanceRejectedEventProcessor())
        .onCommand(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.CANCEL,
            new CancelWorkflowInstanceProcessor())
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN,
            w -> isActive(w.getWorkflowInstanceKey()),
            bpmnAspectProcessor)
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.ACTIVITY_READY,
            w -> isActive(w.getWorkflowInstanceKey()),
            new ActivityReadyEventProcessor())
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.ACTIVITY_ACTIVATED,
            w -> isActive(w.getWorkflowInstanceKey()),
            new ActivityActivatedProcessor())
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.ACTIVITY_COMPLETING,
            w -> isActive(w.getWorkflowInstanceKey()),
            new ActivityCompletingEventProcessor())
        .onCommand(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.UPDATE_PAYLOAD,
            new UpdatePayloadProcessor())
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.START_EVENT_OCCURRED,
            w -> isActive(w.getWorkflowInstanceKey()),
            bpmnAspectProcessor)
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.END_EVENT_OCCURRED,
            w -> isActive(w.getWorkflowInstanceKey()),
            bpmnAspectProcessor)
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.GATEWAY_ACTIVATED,
            w -> isActive(w.getWorkflowInstanceKey()),
            bpmnAspectProcessor)
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.ACTIVITY_COMPLETED,
            w -> isActive(w.getWorkflowInstanceKey()),
            bpmnAspectProcessor)
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.CANCELED,
            (Consumer<WorkflowInstanceRecord>)
                (e) -> workflowInstanceEventCanceled.incrementOrdered())
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.COMPLETED,
            (Consumer<WorkflowInstanceRecord>)
                (e) -> workflowInstanceEventCompleted.incrementOrdered())
        .onEvent(ValueType.JOB, JobIntent.CREATED, new JobCreatedProcessor())
        .onEvent(ValueType.JOB, JobIntent.COMPLETED, new JobCompletedEventProcessor())
        .withStateResource(payloadCache.getMap())
        .withListener(payloadCache)
        .withListener(this)
        .withListener(bpmnAspectProcessor)
        .build();
  }

  @Override
  public void onOpen(TypedStreamProcessor streamProcessor) {

    this.actor = streamProcessor.getActor();
    final LogStream logStream = streamProcessor.getEnvironment().getStream();

    final StreamProcessorContext context = streamProcessor.getStreamProcessorContext();
    final MetricsManager metricsManager = context.getActorScheduler().getMetricsManager();
    final String topicName =
        logStream.getTopicName().getStringWithoutLengthUtf8(0, logStream.getTopicName().capacity());
    final String partitionId = Integer.toString(logStream.getPartitionId());

    workflowInstanceEventCreate =
        metricsManager
            .newMetric("workflow_instance_events_count")
            .type("counter")
            .label("topic", topicName)
            .label("partition", partitionId)
            .label("type", "created")
            .create();

    workflowInstanceEventCanceled =
        metricsManager
            .newMetric("workflow_instance_events_count")
            .type("counter")
            .label("topic", topicName)
            .label("partition", partitionId)
            .label("type", "canceled")
            .create();

    workflowInstanceEventCompleted =
        metricsManager
            .newMetric("workflow_instance_events_count")
            .type("counter")
            .label("topic", topicName)
            .label("partition", partitionId)
            .label("type", "completed")
            .create();
  }

  @Override
  public void onClose() {
    workflowCache.close();
    workflowInstanceEventCreate.close();
    workflowInstanceEventCanceled.close();
    workflowInstanceEventCompleted.close();
  }

  private boolean isActive(long workflowInstanceKey) {
    return workflowInstances.get(workflowInstanceKey) != null;
  }

  private final class CreateWorkflowInstanceEventProcessor
      implements TypedRecordProcessor<WorkflowInstanceRecord> {
    private boolean accepted;
    private final WorkflowInstanceRecord startEventRecord = new WorkflowInstanceRecord();

    private RejectionType rejectionType;
    private String rejectionReason;
    private long requestId;
    private int requestStreamId;

    @Override
    public void processRecord(
        TypedRecord<WorkflowInstanceRecord> command, EventLifecycleContext ctx) {
      final WorkflowInstanceRecord workflowInstanceCommand = command.getValue();

      this.requestId = command.getMetadata().getRequestId();
      this.requestStreamId = command.getMetadata().getRequestStreamId();

      workflowInstanceCommand.setWorkflowInstanceKey(command.getKey());

      accepted = true;
      resolveWorkflowDefinition(workflowInstanceCommand, ctx);
    }

    private void addRequestMetadata(RecordMetadata metadata) {
      metadata.requestId(requestId).requestStreamId(requestStreamId);
    }

    private void resolveWorkflowDefinition(
        WorkflowInstanceRecord command, EventLifecycleContext ctx) {
      final long workflowKey = command.getWorkflowKey();
      final DirectBuffer bpmnProcessId = command.getBpmnProcessId();
      final int version = command.getVersion();

      ActorFuture<ClientResponse> fetchWorkflowFuture = null;

      if (workflowKey <= 0) {
        // by bpmn process id and version
        if (version > 0) {
          final DeployedWorkflow workflowDefinition =
              workflowCache.getWorkflowByProcessIdAndVersion(bpmnProcessId, version);

          if (workflowDefinition != null) {
            command.setWorkflowKey(workflowDefinition.getKey());
            accepted = true;
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
            command
                .setWorkflowKey(workflowDefinition.getKey())
                .setVersion(workflowDefinition.getVersion());
            accepted = true;
          } else {
            fetchWorkflowFuture = workflowCache.fetchLatestWorkflowByBpmnProcessId(bpmnProcessId);
          }
        }
      }

      // by key
      else {
        final DeployedWorkflow workflowDefinition = workflowCache.getWorkflowByKey(workflowKey);

        if (workflowDefinition != null) {
          command
              .setVersion(workflowDefinition.getVersion())
              .setBpmnProcessId(workflowDefinition.getWorkflow().getBpmnProcessId());
          accepted = true;
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
                accepted = false;
                rejectionType = RejectionType.PROCESSING_ERROR;
                rejectionReason = "Could not fetch workflow: " + err.getMessage();
              } else {
                final DeployedWorkflow workflowDefinition =
                    workflowCache.addWorkflow(response.getResponseBuffer());

                if (workflowDefinition != null) {
                  command
                      .setBpmnProcessId(workflowDefinition.getWorkflow().getBpmnProcessId())
                      .setWorkflowKey(workflowDefinition.getKey())
                      .setVersion(workflowDefinition.getVersion());
                  accepted = true;
                } else {
                  accepted = false;
                  rejectionType = RejectionType.BAD_VALUE;
                  rejectionReason = "Workflow is not deployed";
                }
              }

              workflowFetchedFuture.complete(null);
            });
      }
    }

    @Override
    public long writeRecord(TypedRecord<WorkflowInstanceRecord> command, TypedStreamWriter writer) {

      if (accepted) {
        final TypedBatchWriter batchWriter = writer.newBatch();
        batchWriter.addFollowUpEvent(
            command.getKey(),
            WorkflowInstanceIntent.CREATED,
            command.getValue(),
            this::addRequestMetadata);
        addStartEventOccured(batchWriter, command.getValue());
        return batchWriter.write();
      } else {
        return writer.writeRejection(
            command, rejectionType, rejectionReason, this::addRequestMetadata);
      }
    }

    private void addStartEventOccured(
        TypedBatchWriter batchWriter, WorkflowInstanceRecord createCommand) {
      final Workflow workflow =
          workflowCache.getWorkflowByKey(createCommand.getWorkflowKey()).getWorkflow();
      final StartEvent startEvent = workflow.getInitialStartEvent();
      final DirectBuffer activityId = startEvent.getIdAsBuffer();

      startEventRecord
          .setActivityId(activityId)
          .setBpmnProcessId(createCommand.getBpmnProcessId())
          .setPayload(createCommand.getPayload())
          .setScopeKey(createCommand.getWorkflowInstanceKey())
          .setVersion(createCommand.getVersion())
          .setWorkflowInstanceKey(createCommand.getWorkflowInstanceKey())
          .setWorkflowKey(createCommand.getWorkflowKey());
      batchWriter.addNewEvent(WorkflowInstanceIntent.START_EVENT_OCCURRED, startEventRecord);
    }
  }

  private final class WorkflowInstanceCreatedEventProcessor
      implements TypedRecordProcessor<WorkflowInstanceRecord> {
    @Override
    public boolean executeSideEffects(
        TypedRecord<WorkflowInstanceRecord> record, TypedResponseWriter responseWriter) {
      workflowInstanceEventCreate.incrementOrdered();
      return responseWriter.writeRecordUnchanged(record);
    }

    @Override
    public void updateState(TypedRecord<WorkflowInstanceRecord> record) {
      workflowInstances.onWorkflowInstanceCreated(record.getKey());
    }
  }

  private final class WorkflowInstanceRejectedEventProcessor
      implements TypedRecordProcessor<WorkflowInstanceRecord> {
    @Override
    public boolean executeSideEffects(
        TypedRecord<WorkflowInstanceRecord> record, TypedResponseWriter responseWriter) {
      return responseWriter.writeRecordUnchanged(record);
    }
  }

  private final class TakeSequenceFlowAspectHandler extends FlowElementEventProcessor<FlowNode> {
    @Override
    void processFlowElementEvent(WorkflowInstance workflowInstance, Scope scope,
        TypedRecord<WorkflowInstanceRecord> event, FlowNode currentFlowNode) {
      // the activity has exactly one outgoing sequence flow
      final SequenceFlow sequenceFlow = currentFlowNode.getOutgoingSequenceFlows().get(0);

      event.getValue().setActivityId(sequenceFlow.getIdAsBuffer());
    }

    @Override
    public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer) {
      return writer.writeNewEvent(WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN, record.getValue());
    }
  }

  private final class ExclusiveSplitAspectHandler
      extends FlowElementEventProcessor<ExclusiveGateway> {
    private boolean createsIncident;
    private boolean isResolvingIncident;
    private final IncidentRecord incidentCommand = new IncidentRecord();

    @Override
    void processFlowElementEvent(WorkflowInstance workflowInstance, Scope scope,
        TypedRecord<WorkflowInstanceRecord> event, ExclusiveGateway exclusiveGateway) {
      try {
        isResolvingIncident = event.getMetadata().hasIncidentKey();

        final WorkflowInstanceRecord value = event.getValue();
        final SequenceFlow sequenceFlow =
            getSequenceFlowWithFulfilledCondition(exclusiveGateway, value.getPayload());

        if (sequenceFlow != null) {
          value.setActivityId(sequenceFlow.getIdAsBuffer());

          createsIncident = false;
        } else {
          incidentCommand.reset();
          incidentCommand
              .initFromWorkflowInstanceFailure(event)
              .setErrorType(ErrorType.CONDITION_ERROR)
              .setErrorMessage("All conditions evaluated to false and no default flow is set.");

          createsIncident = true;
        }
      } catch (JsonConditionException e) {
        incidentCommand.reset();

        incidentCommand
            .initFromWorkflowInstanceFailure(event)
            .setErrorType(ErrorType.CONDITION_ERROR)
            .setErrorMessage(e.getMessage());

        createsIncident = true;
      }
    }

    private SequenceFlow getSequenceFlowWithFulfilledCondition(
        ExclusiveGateway exclusiveGateway, DirectBuffer payload) {
      final List<SequenceFlow> sequenceFlows =
          exclusiveGateway.getOutgoingSequenceFlowsWithConditions();
      for (int s = 0; s < sequenceFlows.size(); s++) {
        final SequenceFlow sequenceFlow = sequenceFlows.get(s);

        final CompiledJsonCondition compiledCondition = sequenceFlow.getCondition();
        final boolean isFulFilled =
            conditionInterpreter.eval(compiledCondition.getCondition(), payload);

        if (isFulFilled) {
          return sequenceFlow;
        }
      }
      return exclusiveGateway.getDefaultFlow();
    }

    @Override
    public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer) {
      if (!createsIncident) {
        return writer.writeNewEvent(WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN, record.getValue());
      } else {
        if (!isResolvingIncident) {
          return writer.writeNewCommand(IncidentIntent.CREATE, incidentCommand);
        } else {
          return writer.writeFollowUpEvent(
              record.getMetadata().getIncidentKey(),
              IncidentIntent.RESOLVE_FAILED,
              incidentCommand);
        }
      }
    }
  }

  private final class ConsumeTokenAspectHandler extends FlowElementEventProcessor<FlowElement> {
    private boolean isCompleted;
    private int activeTokenCount;

    @Override
    void processFlowElementEvent(WorkflowInstance workflowInstance, Scope scope,
        TypedRecord<WorkflowInstanceRecord> event, FlowElement currentFlowNode) {
      final WorkflowInstanceRecord workflowInstanceEvent = event.getValue();

      isCompleted = true; // TODO: currently always completes the scope
      if (isCompleted) {
        workflowInstanceEvent.setActivityId("");
      }
    }

    @Override
    public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer) {
      if (isCompleted) {
        return writer.writeFollowUpEvent(
            record.getValue().getWorkflowInstanceKey(),
            WorkflowInstanceIntent.COMPLETED,
            record.getValue());
      } else {
        return 0L;
      }
    }

    @Override
    public void updateState(TypedRecord<WorkflowInstanceRecord> record) {
      if (isCompleted) {
        final long workflowInstanceKey = record.getValue().getWorkflowInstanceKey();
        payloadCache.remove(workflowInstanceKey);
        workflowInstances.onWorkflowInstanceFinished(record.getKey());
      }
    }
  }

  private final class ActivityReadyEventProcessor extends FlowElementEventProcessor<FlowNode> {
    private final IncidentRecord incidentCommand = new IncidentRecord();

    private boolean createsIncident;
    private boolean isResolvingIncident;
    private UnsafeBuffer wfInstancePayload = new UnsafeBuffer(0, 0);

    private WorkflowInstance workflowInstance;

    @Override
    void processFlowElementEvent(WorkflowInstance workflowInstance, Scope scope,
        TypedRecord<WorkflowInstanceRecord> event, FlowNode serviceTask) {
      this.workflowInstance = workflowInstance;
      createsIncident = false;
      isResolvingIncident = event.getMetadata().hasIncidentKey();

      final WorkflowInstanceRecord activityEvent = event.getValue();
      wfInstancePayload.wrap(activityEvent.getPayload());

      final Mapping[] inputMappings = serviceTask.getInputOutputMapping().getInputMappings();

      // only if we have no default mapping we have to use the mapping processor
      if (inputMappings.length > 0) {
        try {
          final int resultLen =
              payloadMappingProcessor.extract(activityEvent.getPayload(), inputMappings);
          final MutableDirectBuffer mappedPayload = payloadMappingProcessor.getResultBuffer();
          activityEvent.setPayload(mappedPayload, 0, resultLen);
        } catch (MappingException e) {
          incidentCommand.reset();

          incidentCommand
              .initFromWorkflowInstanceFailure(event)
              .setErrorType(ErrorType.IO_MAPPING_ERROR)
              .setErrorMessage(e.getMessage());

          createsIncident = true;
        }
      }
    }

    @Override
    public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer) {
      if (!createsIncident) {
        return writer.writeFollowUpEvent(
            record.getKey(), WorkflowInstanceIntent.ACTIVITY_ACTIVATED, record.getValue());
      } else {
        if (!isResolvingIncident) {
          return writer.writeNewCommand(IncidentIntent.CREATE, incidentCommand);
        } else {
          return writer.writeFollowUpEvent(
              record.getMetadata().getIncidentKey(),
              IncidentIntent.RESOLVE_FAILED,
              incidentCommand);
        }
      }
    }

    @Override
    public void updateState(TypedRecord<WorkflowInstanceRecord> record) {
      final WorkflowInstanceRecord workflowInstanceEvent = record.getValue();

      // TODO: this is actually too late to make things like cancel see this, but maybe this is okay
      // as soon as cancel propagates down scopes and is therefore split into multiple event cycles
      workflowInstance.newScope(workflowInstanceEvent.getScopeKey(), record.getKey());

      if (!createsIncident) {
        payloadCache.addPayload(
            workflowInstanceEvent.getWorkflowInstanceKey(),
            record.getPosition(),
            wfInstancePayload);
      }
    }
  }

  private final class ServiceTaskActivatedProcessor
      extends FlowElementEventProcessor<ServiceTask> {
    private final JobRecord jobCommand = new JobRecord();

    @Override
    void processFlowElementEvent(WorkflowInstance workflowInstance, Scope scope,
        TypedRecord<WorkflowInstanceRecord> event, ServiceTask serviceTask) {
      final TaskDefinition taskDefinition = serviceTask.getTaskDefinition();

      final WorkflowInstanceRecord value = event.getValue();

      jobCommand.reset();

      jobCommand
          .setType(taskDefinition.getTypeAsBuffer())
          .setRetries(taskDefinition.getRetries())
          .setPayload(value.getPayload())
          .headers()
          .setBpmnProcessId(value.getBpmnProcessId())
          .setWorkflowDefinitionVersion(value.getVersion())
          .setWorkflowKey(value.getWorkflowKey())
          .setWorkflowInstanceKey(value.getWorkflowInstanceKey())
          .setScopeKey(value.getScopeKey())
          .setActivityId(serviceTask.getIdAsBuffer())
          .setActivityInstanceKey(event.getKey());

      final io.zeebe.model.bpmn.instance.TaskHeaders customHeaders = serviceTask.getTaskHeaders();

      if (!customHeaders.isEmpty()) {
        jobCommand.setCustomHeaders(customHeaders.asMsgpackEncoded());
      }
    }

    @Override
    public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer) {
      return writer.writeNewCommand(JobIntent.CREATE, jobCommand);
    }
  }

  private final class JobCreatedProcessor implements TypedRecordProcessor<JobRecord> {
    private Scope scope;

    @Override
    public void processRecord(TypedRecord<JobRecord> record) {

      scope = null;

      final JobHeaders jobHeaders = record.getValue().headers();
      final long activityInstanceKey = jobHeaders.getActivityInstanceKey();
      final long workflowInstanceKey = jobHeaders.getWorkflowInstanceKey();
      if (activityInstanceKey > 0) {
        final WorkflowInstance workflowInstance = workflowInstances.get(workflowInstanceKey);

        scope = workflowInstance.getScope(activityInstanceKey);
      }
    }

    @Override
    public void updateState(TypedRecord<JobRecord> record) {
      if (scope != null) {
        scope.onJobCreated(record.getKey(), record.getPosition());
      }
    }
  }

  private final class JobCompletedEventProcessor implements TypedRecordProcessor<JobRecord> {
    private final WorkflowInstanceRecord workflowInstanceEvent = new WorkflowInstanceRecord();

    private boolean activityCompleted;
    private long activityInstanceKey;

    private Scope scope;

    @Override
    public void processRecord(TypedRecord<JobRecord> record) {
      activityCompleted = false;

      final JobRecord jobEvent = record.getValue();
      final JobHeaders jobHeaders = jobEvent.headers();
      activityInstanceKey = jobHeaders.getActivityInstanceKey();
      final long workflowInstanceKey = jobHeaders.getWorkflowInstanceKey();
      final WorkflowInstance workflowInstance = workflowInstances.get(workflowInstanceKey);


      if (jobHeaders.getWorkflowInstanceKey() > 0) {
        scope = workflowInstance.getScope(activityInstanceKey);

        workflowInstanceEvent
            .setBpmnProcessId(jobHeaders.getBpmnProcessId())
            .setVersion(jobHeaders.getWorkflowDefinitionVersion())
            .setWorkflowKey(jobHeaders.getWorkflowKey())
            .setWorkflowInstanceKey(jobHeaders.getWorkflowInstanceKey())
            .setActivityId(jobHeaders.getActivityId())
            .setScopeKey(jobHeaders.getScopeKey())
            .setPayload(jobEvent.getPayload());

        activityCompleted = true;
      }
    }

    @Override
    public long writeRecord(TypedRecord<JobRecord> record, TypedStreamWriter writer) {
      if (scope != null)
      {
        return writer.writeFollowUpEvent(
            activityInstanceKey,
            WorkflowInstanceIntent.ACTIVITY_COMPLETING,
            workflowInstanceEvent);
      }
      else
      {
        return 0;
      }
    }

    @Override
    public void updateState(TypedRecord<JobRecord> record) {
      if (scope != null)
      {
        scope.onJobFinished(record.getKey());
      }
    }
  }

  private final class ActivityCompletingEventProcessor
      extends FlowElementEventProcessor<FlowNode> {
    private final IncidentRecord incidentCommand = new IncidentRecord();
    private boolean hasIncident;
    private boolean isResolvingIncident;
    private WorkflowInstance workflowInstance;

    @Override
    void processFlowElementEvent(WorkflowInstance workflowInstance, Scope scope,
        TypedRecord<WorkflowInstanceRecord> event, FlowNode serviceTask) {
      this.workflowInstance = workflowInstance;
      hasIncident = false;
      isResolvingIncident = event.getMetadata().hasIncidentKey();

      final WorkflowInstanceRecord activityEvent = event.getValue();
      final DirectBuffer workflowInstancePayload =
          payloadCache.getPayload(activityEvent.getWorkflowInstanceKey());

      final InputOutputMapping inputOutputMapping = serviceTask.getInputOutputMapping();
      tryToExecuteOutputBehavior(event, activityEvent, workflowInstancePayload, inputOutputMapping);
    }

    private void tryToExecuteOutputBehavior(
        TypedRecord<WorkflowInstanceRecord> event,
        WorkflowInstanceRecord activityEvent,
        DirectBuffer workflowInstancePayload,
        InputOutputMapping inputOutputMapping) {
      final OutputBehavior outputBehavior = inputOutputMapping.getOutputBehavior();

      if (outputBehavior == OutputBehavior.NONE) {
        activityEvent.setPayload(workflowInstancePayload);
      } else {
        if (outputBehavior == OutputBehavior.OVERWRITE) {
          workflowInstancePayload = EMPTY_PAYLOAD;
        }

        final Mapping[] outputMappings = inputOutputMapping.getOutputMappings();
        final DirectBuffer jobPayload = activityEvent.getPayload();

        try {
          final int resultLen =
              payloadMappingProcessor.merge(jobPayload, workflowInstancePayload, outputMappings);
          final MutableDirectBuffer mergedPayload = payloadMappingProcessor.getResultBuffer();
          activityEvent.setPayload(mergedPayload, 0, resultLen);
        } catch (MappingException e) {
          createIncident(event, e.getMessage());
          hasIncident = true;
        }
      }
    }

    private void createIncident(TypedRecord<WorkflowInstanceRecord> event, String s) {
      incidentCommand.reset();
      incidentCommand
          .initFromWorkflowInstanceFailure(event)
          .setErrorType(ErrorType.IO_MAPPING_ERROR)
          .setErrorMessage(s);
    }

    @Override
    public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer) {
      if (!hasIncident) {
        return writer.writeFollowUpEvent(
            record.getKey(), WorkflowInstanceIntent.ACTIVITY_COMPLETED, record.getValue());
      } else {
        if (!isResolvingIncident) {
          return writer.writeNewCommand(IncidentIntent.CREATE, incidentCommand);
        } else {
          return writer.writeFollowUpEvent(
              record.getMetadata().getIncidentKey(),
              IncidentIntent.RESOLVE_FAILED,
              incidentCommand);
        }
      }
    }

    @Override
    public void updateState(TypedRecord<WorkflowInstanceRecord> record) {
      if (!hasIncident) {
        workflowInstance.removeScope(record.getKey());
      }
    }
  }

  private final class CancelWorkflowInstanceProcessor
      implements TypedRecordProcessor<WorkflowInstanceRecord> {

    @Override
    public void processRecord(TypedRecord<WorkflowInstanceRecord> command) {
      throw new RuntimeException("Cancel is currently not implemented");
    }

  }

  private final class UpdatePayloadProcessor implements CommandProcessor<WorkflowInstanceRecord> {
    @Override
    public CommandResult onCommand(
        TypedRecord<WorkflowInstanceRecord> command, CommandControl commandControl) {
      final WorkflowInstanceRecord workflowInstanceEvent = command.getValue();

      final boolean isActive = workflowInstances.get(workflowInstanceEvent.getWorkflowInstanceKey()) != null;

      if (isActive) {
        return commandControl.accept(WorkflowInstanceIntent.PAYLOAD_UPDATED);
      } else {
        return commandControl.reject(
            RejectionType.NOT_APPLICABLE, "Workflow instance is not running");
      }
    }

    @Override
    public void updateStateOnAccept(TypedRecord<WorkflowInstanceRecord> command) {
      final WorkflowInstanceRecord workflowInstanceEvent = command.getValue();
      payloadCache.addPayload(
          workflowInstanceEvent.getWorkflowInstanceKey(),
          command.getPosition(),
          workflowInstanceEvent.getPayload());
    }
  }

  public void fetchWorkflow(
      long workflowKey, Consumer<DeployedWorkflow> onFetched, EventLifecycleContext ctx) {
    final ActorFuture<ClientResponse> responseFuture =
        workflowCache.fetchWorkflowByKey(workflowKey);
    final ActorFuture<Void> onCompleted = new CompletableActorFuture<>();

    ctx.async(onCompleted);

    actor.runOnCompletion(
        responseFuture,
        (response, err) -> {
          if (err != null) {
            onCompleted.completeExceptionally(
                new RuntimeException("Could not fetch workflow", err));
          } else {
            try {
              final DeployedWorkflow workflow =
                  workflowCache.addWorkflow(response.getResponseBuffer());

              onFetched.accept(workflow);

              onCompleted.complete(null);
            } catch (Exception e) {
              onCompleted.completeExceptionally(
                  new RuntimeException("Error while processing fetched workflow", e));
            }
          }
        });
  }

  private class StartActivityProcessor extends FlowElementEventProcessor<SequenceFlow>
  {
    @Override
    void processFlowElementEvent(WorkflowInstance workflowInstance, Scope scope,
        TypedRecord<WorkflowInstanceRecord> event,
        SequenceFlow sequenceFlow) {
      final WorkflowInstanceRecord value = event.getValue();
      value.setActivityId(sequenceFlow.getTargetNode().getIdAsBuffer());
    }

    @Override
    public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer) {
      return writer.writeNewEvent(WorkflowInstanceIntent.ACTIVITY_READY, record.getValue());
    }
  }

  private class ActivateGatewayProcessor extends FlowElementEventProcessor<SequenceFlow>
  {
    @Override
    void processFlowElementEvent(WorkflowInstance workflowInstance, Scope scope,
        TypedRecord<WorkflowInstanceRecord> event,
        SequenceFlow sequenceFlow) {
      final WorkflowInstanceRecord value = event.getValue();
      value.setActivityId(sequenceFlow.getTargetNode().getIdAsBuffer());
    }

    @Override
    public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer) {
      return writer.writeNewEvent(WorkflowInstanceIntent.GATEWAY_ACTIVATED, record.getValue());
    }
  }


  private class TriggerNoneEventProcessor extends FlowElementEventProcessor<SequenceFlow>
  {
    @Override
    void processFlowElementEvent(WorkflowInstance workflowInstance, Scope scope,
        TypedRecord<WorkflowInstanceRecord> event,
        SequenceFlow sequenceFlow) {
      final WorkflowInstanceRecord value = event.getValue();
      value.setActivityId(sequenceFlow.getTargetNode().getIdAsBuffer());
    }

    @Override
    public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer) {
      return writer.writeNewEvent(WorkflowInstanceIntent.END_EVENT_OCCURRED, record.getValue());
    }
  }

  private class ParallelSplitProcessor extends FlowElementEventProcessor<FlowNode>
  {

    private List<SequenceFlow> flowsToTake;

    @Override
    void processFlowElementEvent(WorkflowInstance workflowInstance, Scope scope,
        TypedRecord<WorkflowInstanceRecord> event,
        FlowNode currentFlowNode) {
      flowsToTake = currentFlowNode.getOutgoingSequenceFlows();
    }

    @Override
    public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer) {
      final TypedBatchWriter batchWriter = writer.newBatch();
      final WorkflowInstanceRecord value = record.getValue();

      for (int i = 0; i < flowsToTake.size(); i++)
      {
        final SequenceFlow flow = flowsToTake.get(i);
        value.setActivityId(flow.getIdAsBuffer());
        batchWriter.addNewEvent(WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN, value);
      }

      return batchWriter.write();

    }
  }

  private class ParallelMergeProcessor extends FlowElementEventProcessor<SequenceFlow>
  {
    private TypedStreamReader streamReader;

    private boolean merges;
    private Scope scope;

    private MutableDirectBuffer mergedPayload = new ExpandableArrayBuffer();
    // TODO: extra view because merge processor does not work with buffer offset and length; can be avoided
    private UnsafeBuffer mergedPayloadView;
    private List<Long> mergedRecords = new ArrayList<>();

    @Override
    public void onOpen(TypedStreamProcessor streamProcessor) {
      this.streamReader = streamProcessor.getEnvironment().getStreamReader();
    }

    @Override
    void processFlowElementEvent(WorkflowInstance workflowInstance, Scope scope,
        TypedRecord<WorkflowInstanceRecord> record, SequenceFlow sequenceFlow) {

      this.scope = scope;
      final WorkflowInstanceRecord value = record.getValue();

      final FlowNode mergingGateway = sequenceFlow.getTargetNode();

      scope = workflowInstance.getScope(value.getWorkflowInstanceKey());

      final List<SequenceFlow> incomingSequenceFlows = mergingGateway.getIncomingSequenceFlows();

      merges = true;
      for (SequenceFlow flow : incomingSequenceFlows)
      {
        if (flow != sequenceFlow)
        {
          final long tokenPosition = scope.getSuspendedToken(flow.getIdAsBuffer());

          if (tokenPosition > 0)
          {
            mergedRecords.add(tokenPosition);
          }
          else
          {
            merges = false;
            return;
          }
        }
      }

      mergedPayload.putBytes(0, value.getPayload(), 0, value.getPayload().capacity());
      mergedPayloadView = new UnsafeBuffer(mergedPayload, 0, value.getPayload().capacity());

      for (long position : mergedRecords)
      {
        final TypedRecord<WorkflowInstanceRecord> mergingValue =
            streamReader.readValue(position, WorkflowInstanceRecord.class);
        final int resultLength = payloadMappingProcessor.merge(mergedPayloadView, mergingValue.getValue().getPayload());

        mergedPayload.putBytes(0, payloadMappingProcessor.getResultBuffer(), 0, resultLength);
        mergedPayloadView.wrap(mergedPayload, 0, resultLength);
      }

      // TODO: this is a hack to write the next event assuming that in case of a merge, we no longer need
      // the sequence flow id or the previous payload in downstream methods
      record.getValue().setActivityId(mergingGateway.getIdAsBuffer());
      record.getValue().setPayload(mergedPayloadView);
    }

    @Override
    public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer) {
      if (merges)
      {
        // TODO: Muss hier die Gateway-ID setzen; dürfen aber nicht record überschreiben
        return writer.writeNewEvent(WorkflowInstanceIntent.GATEWAY_ACTIVATED, record.getValue());
      }
      else
      {
        return 0;
      }
    }

    @Override
    public void updateState(TypedRecord<WorkflowInstanceRecord> record) {
      if (merges)
      {
        for (long position : mergedRecords)
        {
          scope.consumeSuspendedToken(position);
        }
      }
      else
      {
        scope.suspendToken(record.getValue().getActivityId(), record.getPosition());
      }
    }
  }

  private class SubProcessActivatedProcessor extends FlowElementEventProcessor<SubProcessImpl>
  {

    @Override
    void processFlowElementEvent(WorkflowInstance workflowInstance, Scope scope,
        TypedRecord<WorkflowInstanceRecord> event, SubProcessImpl subprocess) {
      final StartEvent startEvent = subprocess.getInitialStartEvent();

      final WorkflowInstanceRecord value = event.getValue();

      value.setActivityId(startEvent.getIdAsBuffer());
      value.setScopeKey(event.getKey());
    }


    @Override
    public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer) {
      return writer.writeNewEvent(WorkflowInstanceIntent.START_EVENT_OCCURRED, record.getValue());
    }
  }

  private abstract class FlowElementEventProcessor<T extends FlowElement>
      implements TypedRecordProcessor<WorkflowInstanceRecord> {
    private TypedRecord<WorkflowInstanceRecord> event;

    @Override
    public void processRecord(
        TypedRecord<WorkflowInstanceRecord> record, EventLifecycleContext ctx) {
      event = record;
      final long workflowKey = event.getValue().getWorkflowKey();
      final DeployedWorkflow deployedWorkflow = workflowCache.getWorkflowByKey(workflowKey);

      if (deployedWorkflow == null) {
        fetchWorkflow(workflowKey, this::resolveCurrentFlowNode, ctx);
      } else {
        resolveCurrentFlowNode(deployedWorkflow);
      }
    }

    @SuppressWarnings("unchecked")
    private void resolveCurrentFlowNode(DeployedWorkflow deployedWorkflow) {
      final WorkflowInstanceRecord value = event.getValue();
      final DirectBuffer currentActivityId = value.getActivityId();

      final Workflow workflow = deployedWorkflow.getWorkflow();
      final FlowElement flowElement = workflow.findFlowElementById(currentActivityId);

      final WorkflowInstance workflowInstance = workflowInstances.get(value.getWorkflowInstanceKey());
      final Scope scope = workflowInstance.getScope(value.getScopeKey());

      processFlowElementEvent(workflowInstance, scope, event, (T) flowElement);
    }

    abstract void processFlowElementEvent(
        WorkflowInstance workflowInstance, Scope scope, TypedRecord<WorkflowInstanceRecord> event, T currentFlowNode);
  }

  // TODO: delegation aspect can be consolidated with BpmnAspectEventProcessor
  @SuppressWarnings({"rawtypes", "unchecked"})
  private class ActivityActivatedProcessor extends FlowElementEventProcessor<FlowNode>
  {
    private FlowElementEventProcessor delegate;

    private Map<Class<?>, FlowElementEventProcessor> delegates = new HashMap<>();

    ActivityActivatedProcessor()
    {
      delegates.put(ServiceTaskImpl.class, new ServiceTaskActivatedProcessor());
      delegates.put(SubProcessImpl.class, new SubProcessActivatedProcessor());
    }

    @Override
    void processFlowElementEvent(WorkflowInstance workflowInstance, Scope scope,
        TypedRecord<WorkflowInstanceRecord> event, FlowNode currentFlowNode) {

      final Class<? extends FlowElement> flowElementType = currentFlowNode.getClass();
      delegate = delegates.get(flowElementType);

      if (delegate == null)
      {
        throw new RuntimeException("No processor registered for activity of type " + flowElementType);

      }

      delegate.processFlowElementEvent(workflowInstance, scope, event, currentFlowNode);
    }

    @Override
    public boolean executeSideEffects(
        TypedRecord<WorkflowInstanceRecord> record, TypedResponseWriter responseWriter) {
      return delegate.executeSideEffects(record, responseWriter);
    }

    @Override
    public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer) {
      return delegate.writeRecord(record, writer);
    }

    @Override
    public void updateState(TypedRecord<WorkflowInstanceRecord> record) {
      delegate.updateState(record);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private final class BpmnAspectEventProcessor extends FlowElementEventProcessor<FlowElement> {
    private FlowElementEventProcessor delegate;

    protected final Map<BpmnAspect, FlowElementEventProcessor> aspectHandlers;

    private BpmnAspectEventProcessor() {
      aspectHandlers = new EnumMap<>(BpmnAspect.class);

      aspectHandlers.put(BpmnAspect.TAKE_SEQUENCE_FLOW, new TakeSequenceFlowAspectHandler());
      aspectHandlers.put(BpmnAspect.CONSUME_TOKEN, new ConsumeTokenAspectHandler());
      aspectHandlers.put(BpmnAspect.EXCLUSIVE_SPLIT, new ExclusiveSplitAspectHandler());

      aspectHandlers.put(BpmnAspect.PARALLEL_MERGE, new ParallelMergeProcessor());
      aspectHandlers.put(BpmnAspect.PARALLEL_SPLIT, new ParallelSplitProcessor());
      aspectHandlers.put(BpmnAspect.START_ACTIVITY, new StartActivityProcessor());
      aspectHandlers.put(BpmnAspect.TRIGGER_NONE_EVENT, new TriggerNoneEventProcessor());
      aspectHandlers.put(BpmnAspect.ACTIVATE_GATEWAY, new ActivateGatewayProcessor());
    }

    @Override
    public void onOpen(TypedStreamProcessor streamProcessor) {
      aspectHandlers.values().forEach(p -> p.onOpen(streamProcessor));
    }

    @Override
    public void onClose() {
      aspectHandlers.values().forEach(p -> p.onClose());
    }


    @Override
    void processFlowElementEvent(WorkflowInstance workflowInstance, Scope scope,
        TypedRecord<WorkflowInstanceRecord> event, FlowElement currentFlowNode) {
      final BpmnAspect bpmnAspect = currentFlowNode.getBpmnAspect();

      if (bpmnAspect == null)
      {
        throw new RuntimeException("No BPMN Aspect defined for element " + currentFlowNode);
      }

      delegate = aspectHandlers.get(bpmnAspect);

      if (delegate == null)
      {
        throw new RuntimeException("No aspect processor registered for " + bpmnAspect);

      }

      delegate.processFlowElementEvent(workflowInstance, scope, event, currentFlowNode);
    }

    @Override
    public boolean executeSideEffects(
        TypedRecord<WorkflowInstanceRecord> record, TypedResponseWriter responseWriter) {
      return delegate.executeSideEffects(record, responseWriter);
    }

    @Override
    public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer) {
      return delegate.writeRecord(record, writer);
    }

    @Override
    public void updateState(TypedRecord<WorkflowInstanceRecord> record) {
      delegate.updateState(record);
    }
  }
}
