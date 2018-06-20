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
package io.zeebe.broker.workflow;

import io.zeebe.broker.test.EmbeddedBrokerRule;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.MessageSubscriptionIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.test.broker.protocol.clientapi.ClientApiRule;
import io.zeebe.test.broker.protocol.clientapi.TestTopicClient;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.*;
import org.junit.rules.RuleChain;

public class MessageCorrelationTest {

  public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();
  public ClientApiRule apiRule = new ClientApiRule();

  @Rule public RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(apiRule);

  private TestTopicClient testClient;

  @Before
  public void init() {
    testClient = apiRule.topic();
  }

  @Test
  public void shouldEnterMessageCatchEvent() throws Exception {
    // given
    final byte[] bytes =
        Files.readAllBytes(
            Paths.get(getClass().getResource("/workflows/message-correlation.bpmn").toURI()));
    testClient.deployWithResponse(ClientApiRule.DEFAULT_TOPIC_NAME, bytes);

    apiRule.createTopic("order-events", 1);

    // when
    testClient.createWorkflowInstance("Process_1");

    // then
    testClient.receiveFirstWorkflowInstanceEvent(
        WorkflowInstanceIntent.MESSAGE_CATCH_EVENT_ENTERED);

    final TestTopicClient eventTopicClient =
        apiRule.topic(apiRule.getSinglePartitionId("order-events"));
    eventTopicClient
        .receiveRecords()
        .filter(
            r ->
                r.valueType() == ValueType.MESSAGE_SUBSCRIPTION
                    && r.intent() == MessageSubscriptionIntent.SUBSCRIBED)
        .findFirst()
        .get();
  }
}
