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
package io.zeebe.broker.system.management.topics;

import static io.zeebe.protocol.Protocol.DEFAULT_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.system.management.topics.FetchCreatedTopicsResponse.TopicPartitions;
import io.zeebe.broker.test.EmbeddedBrokerRule;
import io.zeebe.test.broker.protocol.clientapi.ClientApiRule;
import io.zeebe.test.broker.protocol.managementApi.ManagementApiRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public class FetchCreatedTopicsRequestTest {

  public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();
  public ClientApiRule clientApiRule = new ClientApiRule(brokerRule::getClientAddress);
  public ManagementApiRule managementApiRule =
      new ManagementApiRule(brokerRule::getManagementAddress);

  @Rule
  public RuleChain ruleChain =
      RuleChain.outerRule(brokerRule).around(clientApiRule).around(managementApiRule);

  final FetchCreatedTopicsRequest request = new FetchCreatedTopicsRequest();
  final FetchCreatedTopicsResponse response = new FetchCreatedTopicsResponse();

  @Test
  public void shouldReturnCreatedTopicWithPartitionIds() {
    // when
    managementApiRule.sendAndAwait(request, response);

    // then
    assertThat(response.getTopics())
        .extracting(TopicPartitions::getTopicName)
        .contains(DEFAULT_TOPIC);

    assertThat(response.getTopics())
        .filteredOn(t -> t.getTopicName().equals(DEFAULT_TOPIC))
        .flatExtracting(TopicPartitions::getPartitionIds)
        .hasSize(1);
  }
}
