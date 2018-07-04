package io.zeebe.broker.workflow.processor.v2;

public enum BpmnStep {

  TAKE_SEQUENCE_FLOW,

  SCOPE_MERGE,

  EXCLUSIVE_SPLIT,

  PARALLEL_SPLIT,

  ACTIVATE_GATEWAY,

  TRIGGER_NONE_EVENT,
  START_ACTIVITY,

  PARALLEL_MERGE,

}
