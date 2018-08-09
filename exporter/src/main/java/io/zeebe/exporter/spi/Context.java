/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.exporter.spi;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.slf4j.Logger;

public interface Context {
  /** @return pre-configured logger for an exporter */
  Logger getLogger();

  /** @return Zeebe working directory * */
  Path getWorkingDirectory();

  /** @return ID of the exporter * */
  String getId();

  /** @return Map of arguments specified in the configuration file */
  Map<String, Object> getArgs();

  /**
   * Exporters must call this back once they have successfully exported events with the position of
   * the latest event they received (in-order of reception).
   *
   * @param position the last successfully exported event position
   */
  void updateLastExportedPosition(long position);

  /**
   * Schedules the task to be ran after a given time.
   *
   * @param task task to be run after the delay
   * @param delay how long to wait before running the task
   */
  void schedule(Runnable task, Duration delay);

  /**
   * Instantiates an object of type T based on the map of arguments. Keys present in the map but not
   * in the class will be ignored; fields present in the class but not in the map will be left
   * untouched and must be given default values by the user if need be.
   *
   * @param configClass the configuration object class
   * @return new instance populated with the exporter configuration
   */
  <T> T configure(Class<T> configClass);
}
