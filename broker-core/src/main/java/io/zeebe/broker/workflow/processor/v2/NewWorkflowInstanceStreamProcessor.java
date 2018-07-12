package io.zeebe.broker.workflow.processor.v2;

import io.zeebe.broker.clustering.base.topology.TopologyManager;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.logstreams.processor.TypedStreamProcessor;
import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.broker.workflow.map.WorkflowCache;
import io.zeebe.broker.workflow.processor.v2.handler.CreateWorkflowInstanceProcessor;
import io.zeebe.broker.workflow.processor.v2.handler.JobCompletedHandler;
import io.zeebe.broker.workflow.processor.v2.handler.WorkflowInstanceCreatedProcessor;
import io.zeebe.broker.workflow.processor.v2.handler.WorkflowInstanceRejectedProcessor;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.transport.ClientTransport;

public class NewWorkflowInstanceStreamProcessor {

  private final ClientTransport managementApiClient;
  private final TopologyManager topologyManager;

  public NewWorkflowInstanceStreamProcessor(ClientTransport managementApiClient, TopologyManager topologyManager)
  {
    this.managementApiClient = managementApiClient;
    this.topologyManager = topologyManager;
  }


  public TypedStreamProcessor createStreamProcessor(TypedStreamEnvironment environment) {


    final WorkflowCache wfCache =
        new WorkflowCache(managementApiClient, topologyManager, environment.getStream().getTopicName());
    final WorkflowInstances wfInstances = new WorkflowInstances();

    final DelegatingRecordProcessor<WorkflowInstanceRecord> stepProcessor = new DelegatingRecordProcessor<>(
          environment, new BpmnStepRecordProcessor(wfCache, wfInstances));

    // TODO: the handlers could be organized in packages according to the entity types they handle (activity instance, workflow instance, ..)

    return environment
        .newStreamProcessor()
        .onCommand(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.CREATE,
            new DelegatingRecordProcessor<>(environment, new CreateWorkflowInstanceProcessor(wfCache)))
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.CREATED,
            new DelegatingRecordProcessor<>(environment, new WorkflowInstanceCreatedProcessor()))
        .onRejection(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.CREATE,
            new DelegatingRecordProcessor<>(environment, new WorkflowInstanceRejectedProcessor()))
//        .onCommand(
//            ValueType.WORKFLOW_INSTANCE,
//            WorkflowInstanceIntent.CANCEL,
//            new CancelWorkflowInstanceProcessor())
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN,
            stepProcessor)
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.ACTIVITY_READY,
            stepProcessor)
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.ACTIVITY_ACTIVATED,
            stepProcessor)
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.ACTIVITY_COMPLETING,
            stepProcessor)
//        .onCommand(
//            ValueType.WORKFLOW_INSTANCE,
//            WorkflowInstanceIntent.UPDATE_PAYLOAD,
//            new UpdatePayloadProcessor())
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.START_EVENT_OCCURRED,
            stepProcessor)
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.END_EVENT_OCCURRED,
            stepProcessor)
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.GATEWAY_ACTIVATED,
            stepProcessor)
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.ACTIVITY_COMPLETED,
            stepProcessor)
//        .onEvent(
//            ValueType.WORKFLOW_INSTANCE,
//            WorkflowInstanceIntent.CANCELED,
//            (Consumer<WorkflowInstanceRecord>)
//                (e) -> workflowInstanceEventCanceled.incrementOrdered())
//        .onEvent(
//            ValueType.WORKFLOW_INSTANCE,
//            WorkflowInstanceIntent.COMPLETED,
//            (Consumer<WorkflowInstanceRecord>)
//                (e) -> workflowInstanceEventCompleted.incrementOrdered())
//        .onEvent(ValueType.JOB, JobIntent.CREATED, new JobCreatedProcessor())
        .onEvent(ValueType.JOB, JobIntent.COMPLETED, new DelegatingRecordProcessor<>(environment, new JobCompletedHandler(wfInstances)))
//        .withStateResource(workflowInstanceIndex.getMap())
//        .withStateResource(activityInstanceMap.getMap())
//        .withStateResource(payloadCache.getMap())
//        .withListener(payloadCache)
//        .withListener(this)
        .build();
  }
}
