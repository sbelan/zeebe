/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.exporter.manager;

import io.zeebe.broker.exporter.ExporterDescriptor;
import io.zeebe.broker.exporter.ExporterEnvironment;
import io.zeebe.broker.exporter.ExporterLoadException;
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
    final ExporterDescriptor container = new ExporterDescriptor(id, exporterClass, args, env);

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
