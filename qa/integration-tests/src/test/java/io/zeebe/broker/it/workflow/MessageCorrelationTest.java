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
package io.zeebe.broker.it.workflow;

import static io.zeebe.test.util.TestUtil.waitUntil;

import java.util.Collections;

import io.zeebe.broker.it.ClientRule;
import io.zeebe.broker.it.EmbeddedBrokerRule;
import io.zeebe.broker.it.util.TopicEventRecorder;
import io.zeebe.client.ZeebeClient;
import io.zeebe.client.api.clients.MessageClient;
import io.zeebe.client.api.clients.WorkflowClient;
import io.zeebe.client.api.events.WorkflowInstanceState;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

public class MessageCorrelationTest
{

    public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();
    public ClientRule clientRule = new ClientRule();
    public TopicEventRecorder eventRecorder = new TopicEventRecorder(clientRule);

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(clientRule).around(eventRecorder);

    @Rule
    public ExpectedException exception = ExpectedException.none();
    private ZeebeClient client;
    private WorkflowClient workflowClient;
    private MessageClient messageClient;

    @Before
    public void init()
    {
        client = clientRule.getClient();
        workflowClient = client.topicClient().workflowClient();
        messageClient = client.topicClient("order-events").messageClient();

        client.newCreateTopicCommand().name("order-events").partitions(5).replicationFactor(1).send().join();

        workflowClient.newDeployCommand().addResourceFromClasspath("workflows/message-correlation.bpmn").send().join();

    }

    @Test
    public void shouldCorrelateMessageToWaitingWorkflowInstance()
    {

        workflowClient.newCreateInstanceCommand().bpmnProcessId("Process_1").latestVersion().payload(Collections.singletonMap("orderId", "123")).send().join();

        waitUntil(() -> eventRecorder.hasWorkflowInstanceEvent(WorkflowInstanceState.MESSAGE_CATCH_EVENT_ENTERED));

        // when
        messageClient.newPublishCommand().messageName("cancel order").messageKey("123").payload(Collections.singletonMap("reason", "foo")).send().join();

        // then
        waitUntil(() -> eventRecorder.hasWorkflowInstanceEvent(WorkflowInstanceState.MESSAGE_CATCH_EVENT_OCCURRED));
        waitUntil(() -> eventRecorder.hasWorkflowInstanceEvent(WorkflowInstanceState.COMPLETED));
    }

    @Test
    public void shouldCorrelateMessageToWorkflowInstanceIfAlreadyReceived()
    {
        messageClient.newPublishCommand().messageName("cancel order").messageKey("123").payload(Collections.singletonMap("reason", "foo")).send().join();

        // when
        workflowClient.newCreateInstanceCommand().bpmnProcessId("Process_1").latestVersion().payload(Collections.singletonMap("orderId", "123")).send().join();

        // then
        waitUntil(() -> eventRecorder.hasWorkflowInstanceEvent(WorkflowInstanceState.MESSAGE_CATCH_EVENT_OCCURRED));
        waitUntil(() -> eventRecorder.hasWorkflowInstanceEvent(WorkflowInstanceState.COMPLETED));
    }

}
