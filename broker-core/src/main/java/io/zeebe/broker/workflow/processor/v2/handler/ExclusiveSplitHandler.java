package io.zeebe.broker.workflow.processor.v2.handler;

import java.util.List;
import org.agrona.DirectBuffer;
import io.zeebe.broker.incident.data.ErrorType;
import io.zeebe.broker.incident.data.IncidentRecord;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.broker.workflow.processor.v2.BpmnStepContext;
import io.zeebe.broker.workflow.processor.v2.BpmnStepHandler;
import io.zeebe.broker.workflow.processor.v2.RecordWriter;
import io.zeebe.model.bpmn.instance.ExclusiveGateway;
import io.zeebe.model.bpmn.instance.SequenceFlow;
import io.zeebe.msgpack.el.CompiledJsonCondition;
import io.zeebe.msgpack.el.JsonConditionException;
import io.zeebe.msgpack.el.JsonConditionInterpreter;
import io.zeebe.protocol.intent.IncidentIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class ExclusiveSplitHandler implements BpmnStepHandler<ExclusiveGateway> {

  private final IncidentRecord incidentCommand = new IncidentRecord();
  private final JsonConditionInterpreter conditionInterpreter = new JsonConditionInterpreter();

  @Override
  public void handle(BpmnStepContext<ExclusiveGateway> context) {


    try {

      final WorkflowInstanceRecord value = context.getCurrentValue();
      final SequenceFlow sequenceFlow =
          getSequenceFlowWithFulfilledCondition(context.getElement(), value.getPayload());

      if (sequenceFlow != null) {
        value.setActivityId(sequenceFlow.getIdAsBuffer());

        takeFlow(context, sequenceFlow);
      } else {
        raiseIncident(context, "All conditions evaluated to false and no default flow is set.");
      }
    } catch (JsonConditionException e) {
      raiseIncident(context, e.getMessage());
    }

  }

  private void takeFlow(BpmnStepContext<ExclusiveGateway> context, SequenceFlow sequenceFlow) {

    final WorkflowInstanceRecord value = context.getCurrentValue();
    value.setActivityId(sequenceFlow.getIdAsBuffer());

    context.getRecordWriter().publish(WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN, value);
  }

  private void raiseIncident(BpmnStepContext<ExclusiveGateway> context, String message) {
    incidentCommand.reset();

    final TypedRecord<WorkflowInstanceRecord> record = context.getCurrentRecord();
    incidentCommand
        .initFromWorkflowInstanceFailure(record)
        .setErrorType(ErrorType.CONDITION_ERROR)
        .setErrorMessage(message);

    final RecordWriter writer = context.getRecordWriter();

    if (record.getMetadata().hasIncidentKey())
    {
      writer.publish(IncidentIntent.CREATE, incidentCommand);
    }
    else
    {
      writer.publish(record.getMetadata().getIncidentKey(), IncidentIntent.RESOLVE_FAILED, incidentCommand);
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
}
