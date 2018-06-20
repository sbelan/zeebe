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
package io.zeebe.broker.message;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.test.EmbeddedBrokerRule;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.intent.MessageIntent;
import io.zeebe.test.broker.protocol.clientapi.*;
import org.junit.*;
import org.junit.rules.RuleChain;

public class MessagePublishTest {

  public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();
  public ClientApiRule apiRule = new ClientApiRule();

  @Rule public RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(apiRule);

  private TestTopicClient testClient;

  @Before
  public void init() {
    testClient = apiRule.topic();
  }

  @Test
  public void shouldPublishMessage() throws Exception {

    // given

    // when
    final ExecuteCommandResponse resp = testClient.publishMessage("ORDER_CANCELLED", "orderId");

    // then
    assertThat(resp.key()).isGreaterThanOrEqualTo(0L);
    assertThat(resp.position()).isGreaterThanOrEqualTo(0L);
    assertThat(resp.sourceRecordPosition()).isEqualTo(resp.key());
    assertThat(resp.partitionId()).isEqualTo(apiRule.getDefaultPartitionId());
    assertThat(resp.recordType()).isEqualTo(RecordType.EVENT);
    assertThat(resp.intent()).isEqualTo(MessageIntent.PUBLISHED);
  }
}
