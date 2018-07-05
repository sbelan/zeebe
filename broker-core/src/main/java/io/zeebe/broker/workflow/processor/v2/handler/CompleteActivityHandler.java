package io.zeebe.broker.workflow.processor.v2.handler;

import static io.zeebe.broker.workflow.data.WorkflowInstanceRecord.EMPTY_PAYLOAD;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import io.zeebe.broker.incident.data.ErrorType;
import io.zeebe.broker.incident.data.IncidentRecord;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.broker.workflow.processor.v2.BpmnStepContext;
import io.zeebe.broker.workflow.processor.v2.BpmnStepHandler;
import io.zeebe.broker.workflow.processor.v2.Lifecycle;
import io.zeebe.model.bpmn.instance.ExclusiveGateway;
import io.zeebe.model.bpmn.instance.InputOutputMapping;
import io.zeebe.model.bpmn.instance.OutputBehavior;
import io.zeebe.model.bpmn.instance.ServiceTask;
import io.zeebe.msgpack.mapping.Mapping;
import io.zeebe.msgpack.mapping.MappingException;
import io.zeebe.msgpack.mapping.MappingProcessor;
import io.zeebe.protocol.intent.IncidentIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class CompleteActivityHandler implements BpmnStepHandler<ServiceTask> {

  private final IncidentRecord incidentCommand = new IncidentRecord();

  private final MappingProcessor payloadMappingProcessor = new MappingProcessor(4096);

  @Override
  public void handle(BpmnStepContext<ServiceTask> context) {
    final InputOutputMapping inputOutputMapping = context.getElement().getInputOutputMapping();

    final OutputBehavior outputBehavior = inputOutputMapping.getOutputBehavior();

    final WorkflowInstanceRecord value = context.getCurrentValue();
    DirectBuffer activityInstancePayload = context.getActivityInstance().getPayload();

    if (outputBehavior == OutputBehavior.NONE) {
      value.setPayload(activityInstancePayload);
    } else {
      if (outputBehavior == OutputBehavior.OVERWRITE) {
        activityInstancePayload = EMPTY_PAYLOAD;
      }

      final Mapping[] outputMappings = inputOutputMapping.getOutputMappings();
      final DirectBuffer jobPayload = value.getPayload();

      try {
        final int resultLen =
            payloadMappingProcessor.merge(jobPayload, activityInstancePayload, outputMappings);
        final MutableDirectBuffer mergedPayload = payloadMappingProcessor.getResultBuffer();

        value.setPayload(mergedPayload, 0, resultLen);
      } catch (MappingException e) {
        raiseIncident(context, e.getMessage());
        return;
      }
    }

    context.getRecordWriter()
      .publish(context.getCurrentRecord().getKey(), WorkflowInstanceIntent.ACTIVITY_COMPLETED, value);
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
