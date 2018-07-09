package io.zeebe.broker.workflow.processor.v2;

import java.util.EnumMap;
import io.zeebe.broker.logstreams.processor.NoopSnapshotSupport;
import io.zeebe.broker.logstreams.processor.TypedEventImpl;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedResponseWriterImpl;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.logstreams.log.LogStreamWriter;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.logstreams.processor.EventLifecycleContext;
import io.zeebe.logstreams.processor.EventProcessor;
import io.zeebe.logstreams.processor.StreamProcessor;
import io.zeebe.logstreams.spi.SnapshotSupport;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.RecordMetadata;
import io.zeebe.util.ReflectUtil;

public class LifecycleProcessor implements StreamProcessor, EventProcessor {

  private final RecordMetadata metadata = new RecordMetadata();

  private EnumMap<ValueType, Lifecycle<?, ?>> lifecycles = new EnumMap<>(ValueType.class);
  private final EnumMap<ValueType, UnpackedObject> eventCache;

  private TypedStreamEnvironment environment;
  private TypedResponseWriter responseWriter;
  private TypedStreamWriter streamWriter;

  // record processing context
  private Lifecycle<?, ?> selectedLifecycle;
  protected final TypedEventImpl typedEvent = new TypedEventImpl();

  private final RecordWriter writer;

  public LifecycleProcessor(TypedStreamEnvironment environment, int partition)
  {
    this.environment = environment;
    this.streamWriter = environment.getStreamWriter();
    this.responseWriter = new TypedResponseWriterImpl(environment.getOutput(), partition);
    eventCache = new EnumMap<>(ValueType.class);
    environment.getEventRegistry().forEach((t, c) -> eventCache.put(t, ReflectUtil.newInstance(c)));
    this.writer = new RecordWriter();
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
    selectedLifecycle.process(writer, typedEvent);
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
