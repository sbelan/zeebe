package io.zeebe.broker.exporter;

import io.zeebe.broker.exporter.jar.JarClassLoader;
import io.zeebe.broker.exporter.jar.JarLoader;
import io.zeebe.broker.system.configuration.ExporterCfg;
import io.zeebe.exporter.spi.Exporter;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class ExporterManager {
  private final ExporterEnvironment env;
  private final Set<ExporterDescriptor> loadedExporters = new HashSet<>();
  private final JarLoader loader = new JarLoader();

  public ExporterManager(final ExporterEnvironment env) {
    this.env = env;
  }

  public void load(List<ExporterCfg> configs) {
    configs.forEach(this::load);
  }

  public void load(final String id, final Class<? extends Exporter> exporterClass) {
    load(id, exporterClass, null);
  }

  public void load(
      final String id,
      final Class<? extends Exporter> exporterClass,
      final Map<String, Object> args) {
    final ExporterContext context = new ExporterContext(id, args, env);
    final ExporterDescriptor container = new ExporterDescriptor(id, exporterClass, args, context);

    try {
      container.verify();
    } catch (IllegalAccessException | InstantiationException e) {
      throw new ExporterLoadException(id, args, e);
    }

    loadedExporters.add(container);
  }

  public void load(final ExporterCfg config) throws ExporterLoadException {
    final Path jarPath = Paths.get(config.getPath());
    final JarClassLoader classLoader = loader.load(jarPath);
    final Class<? extends Exporter> exporterClass;

    try {
      final Class<?> mainClass = classLoader.loadClass(config.getClassName());
      exporterClass = mainClass.asSubclass(Exporter.class);
    } catch (ClassNotFoundException | ClassCastException e) {
      throw new ExporterLoadException(config.getId(), config.getArgs(), e);
    }

    load(config.getId(), exporterClass, config.getArgs());
  }

  public Set<ExporterDescriptor> getLoadedExporters() {
    return Collections.unmodifiableSet(loadedExporters);
  }
}
