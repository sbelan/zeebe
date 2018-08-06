package io.zeebe.broker.exporter;

import io.zeebe.exporter.spi.Argument;
import io.zeebe.exporter.spi.Exporter;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class ExporterDescriptor {
  private static final String NAME_FORMAT = "%s/%s";

  private final String id;
  private final Class<? extends Exporter> exporterClass;
  private final Map<String, Object> args;
  private final String name;
  private final ExporterContext context;

  public ExporterDescriptor(
      final String id,
      final Class<? extends Exporter> exporterClass,
      final Map<String, Object> args,
      final ExporterContext context) {
    this.id = id;
    this.exporterClass = exporterClass;
    this.args = args;
    this.context = context;

    this.name = String.format(NAME_FORMAT, exporterClass.getCanonicalName(), id);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExporterDescriptor that = (ExporterDescriptor) o;
    return Objects.equals(getId(), that.getId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId());
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public ExporterContext getContext() {
    return context;
  }

  // TODO: this sucks
  // just verifies we can create it properly
  public void verify() throws IllegalAccessException, InstantiationException {
    create();
  }

  public Exporter create() throws ExporterLoadException {
    final Field[] fields = exporterClass.getDeclaredFields();
    final Exporter instance;
    try {
      instance = exporterClass.newInstance();
      setArgs(instance, args, fields);
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ExporterLoadException(id, args, e);
    }

    return instance;
  }

  private void setArgs(
      final Exporter instance, final Map<String, Object> args, final Field[] fields)
      throws IllegalAccessException {
    for (final Field field : fields) {
      if (field.isAnnotationPresent(Argument.class)) {
        String arg =
            Optional.of(field.getAnnotation(Argument.class).value()).orElse(field.getName());

        if (args.containsKey(arg)) {
          field.set(instance, args.get(arg));
        }
      }
    }
  }
}
