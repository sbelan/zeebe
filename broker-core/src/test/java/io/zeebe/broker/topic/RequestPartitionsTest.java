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
package io.zeebe.broker.topic;

import static io.zeebe.protocol.Protocol.DEFAULT_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.test.EmbeddedBrokerRule;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.clientapi.ControlMessageType;
import io.zeebe.protocol.clientapi.ErrorCode;
import io.zeebe.test.broker.protocol.clientapi.ClientApiRule;
import io.zeebe.test.broker.protocol.clientapi.ControlMessageResponse;
import io.zeebe.test.broker.protocol.clientapi.ErrorResponse;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

@SuppressWarnings("unchecked")
public class RequestPartitionsTest {
  public static final int EXPECTED_TOTAL_PARTITIONS = 1;
  public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();
  public ClientApiRule apiRule = new ClientApiRule(brokerRule::getClientAddress);

  @Rule public RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(apiRule);

  @Test
  public void shouldReturnCreatedPartitions() {
    // given
    apiRule.waitForTopic(EXPECTED_TOTAL_PARTITIONS);

    // when
    final ControlMessageResponse response =
        apiRule
            .createControlMessageRequest()
            .messageType(ControlMessageType.REQUEST_PARTITIONS)
            .partitionId(Protocol.SYSTEM_PARTITION)
            .sendAndAwait();

    // then
    assertResponse(response, EXPECTED_TOTAL_PARTITIONS, DEFAULT_TOPIC);
  }

  /** testing snapshotting */
  @Test
  public void shouldReturnCreatedPartitionsAfterRestart() {
    // given
    apiRule.waitForTopic(EXPECTED_TOTAL_PARTITIONS);

    brokerRule.restartBroker();

    // when
    // have to do this multiple times as the stream processor for answering the request may not be
    // available yet
    apiRule.waitForTopic(EXPECTED_TOTAL_PARTITIONS);

    // then
    assertResponse(apiRule.requestPartitions(), EXPECTED_TOTAL_PARTITIONS, DEFAULT_TOPIC);
  }

  @Test
  public void shouldRespondWithErrorWhenRequestAddressesNonSystemPartition() {
    // given

    // when
    // have to do this multiple times as the stream processor for answering the request may not be
    // available yet
    final ErrorResponse errorResponse =
        apiRule
            .createControlMessageRequest()
            .messageType(ControlMessageType.REQUEST_PARTITIONS)
            .partitionId(Protocol.SYSTEM_PARTITION + 1)
            .send()
            .awaitError();

    // then
    assertThat(errorResponse.getErrorCode()).isEqualTo(ErrorCode.REQUEST_PROCESSING_FAILURE);
    assertThat(errorResponse.getErrorData())
        .isEqualTo(
            "Partitions request must address the system partition " + Protocol.SYSTEM_PARTITION);
  }

  private void assertResponse(
      final ControlMessageResponse response,
      final int expectedTotalPartitions,
      final String... expectedTopics) {
    final Map<String, Object> responseData = response.getData();
    assertThat(responseData).hasSize(1);
    final List<Map<String, Object>> partitions =
        (List<Map<String, Object>>) responseData.get("partitions");
    assertThat(partitions).isNotNull();
    assertThat(partitions.size())
        .isGreaterThanOrEqualTo(expectedTotalPartitions); // system partition included
    assertThat(partitions).extracting("topic").contains((Object[]) expectedTopics);
    assertThat(partitions).extracting("id").doesNotHaveDuplicates();
  }
}
