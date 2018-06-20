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
package io.zeebe.broker.workflow.correlation;

import io.zeebe.broker.clustering.orchestration.state.KnownTopics;
import io.zeebe.servicecontainer.*;

public class FetchCreatedTopicsRequestHandlerService
    implements Service<FetchCreatedTopicsRequestHandlerService> {

  private final Injector<FetchCreatedTopicsRequestHandlerManager> managerInjector =
      new Injector<>();

  private final Injector<KnownTopics> knownTopicsInjector = new Injector<>();

  @Override
  public void start(ServiceStartContext startContext) {
    final FetchCreatedTopicsRequestHandlerManager manager = getManagerInjector().getValue();

    final KnownTopics knownTopics = getKnownTopicsInjector().getValue();

    final FetchCreatedTopicsRequestHandler handler =
        new FetchCreatedTopicsRequestHandler(knownTopics);

    manager.setFetchCreatedTopicsRequestHandler(handler);
  }

  @Override
  public FetchCreatedTopicsRequestHandlerService get() {
    return this;
  }

  public Injector<FetchCreatedTopicsRequestHandlerManager> getManagerInjector() {
    return managerInjector;
  }

  public Injector<KnownTopics> getKnownTopicsInjector() {
    return knownTopicsInjector;
  }
}
