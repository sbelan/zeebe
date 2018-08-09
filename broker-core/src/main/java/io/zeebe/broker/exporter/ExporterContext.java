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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import io.zeebe.exporter.spi.Context;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.slf4j.Logger;

public class ExporterContext implements Context {
  private static final Gson CONFIG_CREATOR = new Gson();

  private final String id;
  private final Map<String, Object> args;
  private final ExporterEnvironment env;
  private final Consumer<Long> lastPositionUpdater;

  // TODO: potentially have reference to ActorControl
  private final BiConsumer<Runnable, Duration> scheduler;

  private JsonElement intermediateConfig;

  public ExporterContext(
      final String id,
      final Map<String, Object> args,
      final ExporterEnvironment env,
      Consumer<Long> lastPositionUpdater,
      BiConsumer<Runnable, Duration> scheduler) {
    this.id = id;
    this.args = args;
    this.env = env;
    this.lastPositionUpdater = lastPositionUpdater;
    this.scheduler = scheduler;
  }

  @Override
  public Logger getLogger() {
    return env.getLogger();
  }

  @Override
  public Path getWorkingDirectory() {
    return env.getWorkingDirectory();
  }

  @Override
  public Map<String, Object> getArgs() {
    return args;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public void updateLastExportedPosition(long position) {
    lastPositionUpdater.accept(position);
  }

  @Override
  public void schedule(final Runnable task, final Duration delay) {
    scheduler.accept(task, delay);
  }

  @Override
  public <T> T configure(Class<T> configClass) {
    return CONFIG_CREATOR.fromJson(getIntermediateConfig(), configClass);
  }

  private JsonElement getIntermediateConfig() {
    if (intermediateConfig == null) {
      intermediateConfig = CONFIG_CREATOR.toJsonTree(args);
    }

    return intermediateConfig;
  }
}
