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

public interface Exporter {
  /** @return configuration to use for this exporter */
  Configuration getConfiguration();

  /** Starts the exporter. After this returns, the exporter will start receiving events. */
  default void start(Context context) {}

  /** Stops the exporter. After this returns, the exporter will not be receiving events anymore. */
  default void stop() {}

  /** @param event the event to export */
  void export(Event event);
}
