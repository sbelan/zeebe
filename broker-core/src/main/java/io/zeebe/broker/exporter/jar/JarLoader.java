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
package io.zeebe.broker.exporter.jar;

import io.zeebe.broker.Loggers;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import org.slf4j.Logger;

/**
 * Loads JARs and keeps a cache of loaded JARs => JarClassLoader, allowing easy reuse without
 * consuming more resources.
 *
 * <p>Not thread-safe.
 */
public class JarLoader {
  private static final Logger LOG = Loggers.EXPORTERS;

  private final Map<Path, JarClassLoader> cache = new HashMap<>();
  private final JarFilter filter = new JarFilter();

  public JarClassLoader load(final Path jarPath) {
    if (jarPath.endsWith(JarFilter.EXTENSION)) {
      throw new IllegalArgumentException("can only load JARs");
    }

    if (!cache.containsKey(jarPath)) {
      final JarClassLoader classLoader = createClassLoader(jarPath);
      cache.put(jarPath, classLoader);
      return classLoader;
    }

    return cache.get(jarPath);
  }

  private JarClassLoader createClassLoader(final Path jarPath) {
    try {
      final URL jarUrl = jarPath.toUri().toURL();
      return new JarClassLoader(jarUrl);
    } catch (MalformedURLException e) {
      LOG.error("cannot load given JAR at {}", jarPath, e);
      throw new IllegalArgumentException("JAR could not be loaded", e);
    }
  }

  private static class JarFilter implements Predicate<Path> {
    private static final String EXTENSION = ".jar";

    @Override
    public boolean test(Path path) {
      return path.endsWith(EXTENSION);
    }
  }
}
