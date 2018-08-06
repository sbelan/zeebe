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

import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.util.sched.Actor;

public class ExporterService extends Actor implements Service<ExporterDescriptor> {
  private final ExporterDescriptor descriptor;

  public ExporterService(final ExporterDescriptor descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public String getName() {
    return descriptor.getName();
  }

  @Override
  protected void onActorStarting() {
    super.onActorStarting();
  }

  @Override
  protected void onActorStarted() {
    super.onActorStarted();
  }

  @Override
  protected void onActorClosing() {
    super.onActorClosing();
  }

  @Override
  public void start(ServiceStartContext startContext) {}

  @Override
  public ExporterDescriptor get() {
    return descriptor;
  }
}
