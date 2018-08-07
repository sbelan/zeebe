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
package io.zeebe.broker.exporter;

import io.zeebe.exporter.spi.Argument;
import io.zeebe.exporter.spi.Exporter;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Objects;

public class ExporterDescriptor {
  private static final String NAME_FORMAT = "exporter-%s";

  private final String id;
  private final Class<? extends Exporter> exporterClass;
  private final Map<String, Object> args;
  private final String name;
  private final ExporterEnvironment env;

  public ExporterDescriptor(
      final String id,
      final Class<? extends Exporter> exporterClass,
      final Map<String, Object> args,
      final ExporterEnvironment env) {
    this.id = id;
    this.exporterClass = exporterClass;
    this.args = args;
    this.env = env;

    this.name = String.format(NAME_FORMAT, id);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ExporterDescriptor other = (ExporterDescriptor) o;
    return Objects.equals(getId(), other.getId());
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

  public Map<String, Object> getArgs() {
    return args;
  }

  public ExporterEnvironment getEnv() {
    return env;
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
        String arg = field.getAnnotation(Argument.class).value();
        if (arg.equals(Argument.DEFAULT)) {
          arg = field.getName();
        }

        if (args.containsKey(arg)) {
          final Object value = args.get(arg);
          final Class<?> fieldClass = value.getClass();

          if (fieldClass.isAssignableFrom(field.getType())) {
            field.setAccessible(true);
            field.set(instance, fieldClass.cast(value));
            field.get(instance);
          }
        }
      }
    }
  }
}
