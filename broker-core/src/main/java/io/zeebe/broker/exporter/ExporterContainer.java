package io.zeebe.broker.exporter;

import io.zeebe.broker.system.configuration.ExporterCfg;
import io.zeebe.exporter.spi.ConfigArgument;
import io.zeebe.exporter.spi.Exporter;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;

public class ExporterContainer {
  private final Class<? extends Exporter> exporterClass;
  private final ExporterCfg config;

  private Exporter instance;

  public ExporterContainer(
      final Class<? extends Exporter> exporterClass, final ExporterCfg config) {
    this.exporterClass = exporterClass;
    this.config = config;
  }

  // TODO: this sucks
  // just verifies we can create it properly
  public void verify() throws IllegalAccessException, InstantiationException {
    create();
  }

  public Exporter create() throws IllegalAccessException, InstantiationException {
    if (instance != null) {
      return instance;
    }

    final Map<String, Object> args = config.getArgs();
    final Field[] fields = exporterClass.getDeclaredFields();
    final Exporter instance = exporterClass.newInstance();

    setArgs(instance, args, fields);
    return instance;
  }

  private void setArgs(
      final Exporter instance, final Map<String, Object> args, final Field[] fields)
      throws IllegalAccessException {
    for (final Field field : fields) {
      if (field.isAnnotationPresent(ConfigArgument.class)) {
        String arg =
            Optional.of(field.getAnnotation(ConfigArgument.class).value()).orElse(field.getName());

        if (args.containsKey(arg)) {
          field.set(instance, args.get(arg));
        }
      }
    }
  }
}
