package io.zeebe.broker.workflow.processor.v2;

import java.util.EnumMap;
import io.zeebe.broker.clustering.base.topology.TopologyManager;
import io.zeebe.broker.logstreams.processor.NoopSnapshotSupport;
import io.zeebe.broker.logstreams.processor.TypedEventImpl;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.workflow.map.WorkflowCache;
import io.zeebe.logstreams.log.LogStreamWriter;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.logstreams.processor.EventLifecycleContext;
import io.zeebe.logstreams.processor.EventProcessor;
import io.zeebe.logstreams.processor.StreamProcessor;
import io.zeebe.logstreams.processor.StreamProcessorContext;
import io.zeebe.logstreams.spi.SnapshotSupport;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.RecordMetadata;
import io.zeebe.transport.ClientTransport;
import io.zeebe.util.ReflectUtil;

public class LifecycleProcessor implements StreamProcessor, EventProcessor {

  private final RecordMetadata metadata = new RecordMetadata();

  private EnumMap<ValueType, Lifecycle<?, ?>> lifecycles = new EnumMap<>(ValueType.class);
  private final EnumMap<ValueType, UnpackedObject> eventCache;


  // record processing context
  private Lifecycle<?, ?> selectedLifecycle;
  protected final TypedEventImpl typedEvent = new TypedEventImpl();

  private final RecordWriter writer;

  // for workflow fetching
  private final TopologyManager topologyManager;
  private final ClientTransport clientTransport;

  public LifecycleProcessor(TypedStreamEnvironment environment,
      int partition,
      TopologyManager topologyManager,
      ClientTransport clientTransport)
  {
    eventCache = new EnumMap<>(ValueType.class);
    environment.getEventRegistry().forEach((t, c) -> eventCache.put(t, ReflectUtil.newInstance(c)));
    this.writer = new RecordWriter(environment.getStreamWriter(), new ResponseWriter(environment.getOutput(), partition));
    this.topologyManager = topologyManager;
    this.clientTransport = clientTransport;
  }

  public void addLifecycle(ValueType type, Lifecycle<?, ?> lifecycle)
  {
    lifecycles.put(type, lifecycle);
  }

  @Override
  public SnapshotSupport getStateResource() {
    // TODO use concept from typedrecordprocessor here
    return new NoopSnapshotSupport();
  }

  @Override
  public void onOpen(StreamProcessorContext context) {
    final WorkflowCache wfCache = new WorkflowCache(clientTransport, topologyManager, context.getLogStream().getTopicName());

    final WorkflowInstances wfInstances = new WorkflowInstances();

    addLifecycle(ValueType.WORKFLOW_INSTANCE, new WorkflowInstanceLifecycle(wfCache, wfInstances));
    addLifecycle(ValueType.JOB, new JobLifecycle(wfInstances));

    lifecycles.values().forEach(l -> l.onOpen(context.getActorControl()));
  }

  @Override
  public EventProcessor onEvent(LoggedEvent event) {

    writer.reset();

    metadata.wrap(event.getMetadata(), event.getMetadataOffset(), event.getMetadataLength());

    final ValueType valueType = metadata.getValueType();

    if (lifecycles.containsKey(valueType))
    {
      selectedLifecycle = lifecycles.get(valueType);
      final UnpackedObject value = eventCache.get(valueType);
      value.wrap(event.getValueBuffer(), event.getValueOffset(), event.getValueLength());

      typedEvent.wrap(event, metadata, value);

      return this;
    }
    else
    {
      return null;
    }
  }

  @Override
  public void processEvent(EventLifecycleContext ctx) {
    selectedLifecycle.process(writer, typedEvent, ctx);
  }

  @Override
  public long writeEvent(LogStreamWriter writer) {
    return this.writer.flushEvents();
  }

  @Override
  public boolean executeSideEffects() {
    return writer.flushSideEffects();
  }

}
