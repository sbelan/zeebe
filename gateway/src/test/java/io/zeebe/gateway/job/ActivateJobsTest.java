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
package io.zeebe.gateway.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.zeebe.gateway.api.events.JobEvent;
import io.zeebe.gateway.api.record.Record;
import io.zeebe.gateway.util.ClientRule;
import io.zeebe.test.broker.protocol.brokerapi.StubBrokerRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

public class ActivateJobsTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ClientRule clientRule = new ClientRule();
  private StubBrokerRule brokerRule = new StubBrokerRule();

  @Rule public RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(clientRule);

  @Rule public ExpectedException exception = ExpectedException.none();

  @Test
  @Ignore
  public void shouldActiveJobs() throws IOException {
    // given
    final String type = "myJob";
    final String worker = "myWorker";
    final long timeout = ThreadLocalRandom.current().nextLong();
    final int amount = 5;

    // when
  }

  private List<Map<String, Object>> toMapFromRecords(List<JobEvent> records) throws IOException {
    final List<Map<String, Object>> mapped = new ArrayList<>(records.size());

    for (Record record : records) {
      final String json = record.toJson();
      mapped.add(MAPPER.readValue(json, Map.class));
    }

    return mapped;
  }
}
