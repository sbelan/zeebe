package io.zeebe.broker.workflow.processor.v2.handler;

import org.agrona.DirectBuffer;
import io.zeebe.broker.incident.data.ErrorType;
import io.zeebe.broker.incident.data.IncidentRecord;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.broker.workflow.processor.v2.BpmnStepContext;
import io.zeebe.broker.workflow.processor.v2.BpmnStepHandler;
import io.zeebe.broker.workflow.processor.v2.Lifecycle;
import io.zeebe.model.bpmn.instance.ServiceTask;
import io.zeebe.msgpack.mapping.Mapping;
import io.zeebe.msgpack.mapping.MappingException;
import io.zeebe.msgpack.mapping.MappingProcessor;
import io.zeebe.protocol.intent.IncidentIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class ActivateActivityHandler implements BpmnStepHandler<ServiceTask> {

  private final IncidentRecord incidentCommand = new IncidentRecord();
  private final MappingProcessor payloadMappingProcessor = new MappingProcessor(4096);

  @Override
  public void handle(BpmnStepContext<ServiceTask> context) {

    final WorkflowInstanceRecord value = context.getCurrentValue();

    final Mapping[] inputMappings = context.getElement().getInputOutputMapping().getInputMappings();

    // only if we have no default mapping we have to use the mapping processor
    if (inputMappings.length > 0) {
      try {
        final int resultLen =
            payloadMappingProcessor.extract(value.getPayload(), inputMappings);
        final DirectBuffer activityInstancePayload = payloadMappingProcessor.getResultBuffer();
        value.setPayload(activityInstancePayload, 0, resultLen);
      } catch (MappingException e) {
        raiseIncident(context, e.getMessage());
        return;
      }
    }

    context.getActivityInstance().setPayload(value.getPayload());

    context.getRecordWriter().publish(context.getCurrentRecord().getKey(),
        WorkflowInstanceIntent.ACTIVITY_ACTIVATED,
        value);
  }

  private void raiseIncident(BpmnStepContext<ServiceTask> context, String message) {
    incidentCommand.reset();

    final TypedRecord<WorkflowInstanceRecord> record = context.getCurrentRecord();
    incidentCommand
        .initFromWorkflowInstanceFailure(record)
        .setErrorType(ErrorType.CONDITION_ERROR)
        .setErrorMessage(message);

    final Lifecycle writer = context.getRecordWriter();

    if (record.getMetadata().hasIncidentKey())
    {
      writer.publish(IncidentIntent.CREATE, incidentCommand);
    }
    else
    {
      writer.publish(record.getMetadata().getIncidentKey(), IncidentIntent.RESOLVE_FAILED, incidentCommand);
    }

  }

}
