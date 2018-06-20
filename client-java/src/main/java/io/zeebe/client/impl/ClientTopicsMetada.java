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
package io.zeebe.client.impl;

import io.zeebe.client.api.commands.Topic;
import io.zeebe.client.api.commands.Topics;
import io.zeebe.client.impl.topic.TopicsRequestImpl;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

public class ClientTopicsMetada {

  private static final Logger LOG = Loggers.CLIENT_LOGGER;

  private volatile Map<String, Topic> topics = new HashMap<>();

  private final RequestManager requestManager;

  public ClientTopicsMetada(RequestManager requestManager) {
    this.requestManager = requestManager;
  }

  public Topic getTopic(String name, long deadline) {

    long refreshBackoff = 1;

    do {
      final Topic topic = topics.get(name);

      if (topic != null) {
        return topic;
      } else {
        try {
          Thread.sleep(refreshBackoff);
        } catch (InterruptedException e) {
          return null;
        }

        refresh();

        refreshBackoff += Math.min(refreshBackoff * 2, 1000);
      }
    } while (System.currentTimeMillis() < deadline);

    return null;
  }

  private void refresh() {
    try {
      final Topics topics = new TopicsRequestImpl(requestManager).send().join(5, TimeUnit.SECONDS);

      final Map<String, Topic> mappedTopics = new HashMap<>();

      for (Topic t : topics.getTopics()) {
        mappedTopics.put(t.getName(), t);
      }

      this.topics = mappedTopics;
    } catch (RuntimeException e) {
      LOG.debug("Exception while fetching topic metadata", e);
    }
  }
}
