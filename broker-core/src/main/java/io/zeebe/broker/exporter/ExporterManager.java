package io.zeebe.broker.exporter;

import io.zeebe.broker.system.configuration.ExporterCfg;
import io.zeebe.exporter.spi.Exporter;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExporterManager {
  private final Map<String, ExporterContainer> loadedExporters = new HashMap<>();
  private final JarLoader loader = new JarLoader();
  private final ExporterContext context;

  public ExporterManager(final ExporterContext context) {
    this.context = context;
  }

  public void load(List<ExporterCfg> configs) {
    configs.forEach(this::load);
  }

  public void load(final ExporterCfg config) throws ExporterLoadException {
    if (loadedExporters.containsKey(config.getId())) {
      return;
    }

    final Path jarPath = Paths.get(config.getPath());
    final ExporterClassLoader classLoader = loader.load(jarPath);

    try {
      final Class<?> mainClass = classLoader.loadClass(config.getClassName());
      final Class<? extends Exporter> exporterClass = mainClass.asSubclass(Exporter.class);
      final ExporterContainer container = new ExporterContainer(exporterClass, config);

      container.verify();
      loadedExporters.put(config.getId(), container);
    } catch (ClassNotFoundException
        | ClassCastException
        | IllegalAccessException
        | InstantiationException e) {
      throw new ExporterLoadException(config, e);
    }
  }
}
