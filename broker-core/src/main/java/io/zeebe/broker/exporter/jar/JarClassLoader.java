package io.zeebe.broker.exporter.jar;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Provides a class loader which isolates the Exporter, exposing only: system classes,
 * io.zeebe.exporter.* classes, all classes contained in the exporter's JAR
 */
public class JarClassLoader extends URLClassLoader {
  private static final String JAVA_PACKAGE_PREFIX = "java.";
  private static final String EXPORTER_PACKAGE_PREFIX = "io.zeebe.exporter.";

  public JarClassLoader(final URL jarUrl) {
    super(new URL[] {jarUrl});
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    synchronized (getClassLoadingLock(name)) {
      if (name.startsWith(JAVA_PACKAGE_PREFIX)) {
        return findSystemClass(name);
      }

      if (name.startsWith(EXPORTER_PACKAGE_PREFIX)) {
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
