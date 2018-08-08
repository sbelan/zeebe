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

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;

/**
 * Provides a class loader which isolates the Exporter, exposing only: system classes,
 * io.zeebe.exporter.* classes, all classes contained in the exporter's JAR
 */
public class JarClassLoader extends URLClassLoader {
  private static final String JAR_URL_FORMAT = "jar:%s!/";
  private static final String JAVA_PACKAGE_PREFIX = "java.";
  private static final String EXPORTER_PACKAGE_PREFIX = "io.zeebe.exporter.";
  private static final String SLF4J_PACKAGE_PREFIX = "org.slf4j.";
  private static final String LOG4J_PACKAGE_PREFIX = "org.apache.logging.log4j.";

  public JarClassLoader(final URL jarUrl) {
    super(new URL[] {jarUrl});
  }

  public JarClassLoader(final Path jarPath) throws MalformedURLException {
    this(JarClassLoader.jarUrl(jarPath));
  }

  private static URL jarUrl(final Path jarPath) throws MalformedURLException {
    final String jarUrl = jarPath.toUri().toURL().toString();
    return new URL(String.format(JAR_URL_FORMAT, jarUrl));
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    synchronized (getClassLoadingLock(name)) {
      if (name.startsWith(JAVA_PACKAGE_PREFIX)) {
        return findSystemClass(name);
      }

      if (name.startsWith(EXPORTER_PACKAGE_PREFIX)
          || name.startsWith(LOG4J_PACKAGE_PREFIX)
          || name.startsWith(SLF4J_PACKAGE_PREFIX)) {
        return getClass().getClassLoader().loadClass(name);
      }

      Class<?> clazz = findLoadedClass(name);
      if (clazz == null) {
        try {
          clazz = findClass(name);
        } catch (final ClassNotFoundException ex) {
          clazz = super.loadClass(name);
        }
      }

      return clazz;
    }
  }
}
