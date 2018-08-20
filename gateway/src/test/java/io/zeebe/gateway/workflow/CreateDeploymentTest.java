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
package io.zeebe.gateway.workflow;

import static io.zeebe.protocol.Protocol.DEFAULT_TOPIC;
import static io.zeebe.protocol.Protocol.DEPLOYMENT_PARTITION;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.gateway.ZeebeClient;
import io.zeebe.gateway.api.commands.Workflow;
import io.zeebe.gateway.api.events.DeploymentEvent;
import io.zeebe.gateway.cmd.ClientCommandRejectedException;
import io.zeebe.gateway.util.ClientRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.DeploymentIntent;
import io.zeebe.test.broker.protocol.brokerapi.ExecuteCommandRequest;
import io.zeebe.test.broker.protocol.brokerapi.StubBrokerRule;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

public class CreateDeploymentTest {
  private static final BpmnModelInstance WORKFLOW_MODEL =
      Bpmn.createExecutableProcess("process").startEvent().done();

  private static final byte[] WORKFLOW_AS_BYTES =
      Bpmn.convertToString(WORKFLOW_MODEL).getBytes(UTF_8);

  public StubBrokerRule brokerRule = new StubBrokerRule();
  public ClientRule clientRule = new ClientRule(brokerRule);

  @Rule public RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(clientRule);

  @Rule public ExpectedException thrown = ExpectedException.none();

  protected ZeebeClient client;

  @Before
  public void setUp() {
    this.client = clientRule.getClient();
  }

  @Test
  public void shouldSendDeploymentRequestToDefaultTopic() {
    // given
    brokerRule.deployments().registerCreateCommand();

    // when
    final DeploymentEvent deployment =
        client
            .topicClient()
            .workflowClient()
            .newDeployCommand()
            .addWorkflowModel(WORKFLOW_MODEL, "model.bpmn")
            .send()
            .join();

    // then
    assertThat(brokerRule.getReceivedCommandRequests()).hasSize(1);

    final ExecuteCommandRequest commandRequest = brokerRule.getReceivedCommandRequests().get(0);
    assertThat(commandRequest.partitionId()).isEqualTo(DEPLOYMENT_PARTITION);

    assertThat(deployment.getDeploymentTopic()).isEqualTo(DEFAULT_TOPIC);
    assertThat(deployment.getMetadata().getTopicName()).isEqualTo(DEFAULT_TOPIC);
    assertThat(deployment.getMetadata().getSourceRecordPosition()).isEqualTo(1L);
  }

  @Test
  public void shouldCreateDeployment() {
    // given
    final List<Map<String, Object>> deployedWorkflows = new ArrayList<>();
    Map<String, Object> deployedWorkflow = new HashMap<>();
    deployedWorkflow.put("bpmnProcessId", "foo");
    deployedWorkflow.put("version", 1);
    deployedWorkflow.put("workflowKey", 2);
    deployedWorkflow.put("resourceName", "foo.bpmn");
    deployedWorkflows.add(deployedWorkflow);

    deployedWorkflow = new HashMap<>();
    deployedWorkflow.put("bpmnProcessId", "bar");
    deployedWorkflow.put("version", 2);
    deployedWorkflow.put("workflowKey", 3);
    deployedWorkflow.put("resourceName", "bar.bpmn");
    deployedWorkflows.add(deployedWorkflow);

    brokerRule
        .deployments()
        .registerCreateCommand(
            b ->
                b.sourceRecordPosition(1L)
                    .key(2L)
                    .value()
                    .put("deployedWorkflows", deployedWorkflows)
                    .done());

    // when
    final DeploymentEvent deployment =
        clientRule
            .workflowClient()
            .newDeployCommand()
            .addWorkflowModel(WORKFLOW_MODEL, "model.bpmn")
            .send()
            .join();

    // then
    assertThat(brokerRule.getReceivedCommandRequests()).hasSize(1);

    final ExecuteCommandRequest commandRequest = brokerRule.getReceivedCommandRequests().get(0);
    assertThat(commandRequest.valueType()).isEqualTo(ValueType.DEPLOYMENT);
    assertThat(commandRequest.intent()).isEqualTo(DeploymentIntent.CREATE);
    assertThat(commandRequest.key()).isEqualTo(-1);

    assertThat(deployment.getMetadata().getKey()).isEqualTo(2L);
    assertThat(deployment.getMetadata().getTopicName()).isEqualTo(DEFAULT_TOPIC);
    assertThat(deployment.getMetadata().getPartitionId()).isEqualTo(DEPLOYMENT_PARTITION);
    assertThat(deployment.getMetadata().getSourceRecordPosition()).isEqualTo(1L);

    assertThat(deployment.getDeployedWorkflows()).hasSize(2);
    assertThat(deployment.getDeployedWorkflows())
        .extracting(Workflow::getBpmnProcessId)
        .contains("foo", "bar");
    assertThat(deployment.getDeployedWorkflows()).extracting(Workflow::getVersion).contains(1, 2);
    assertThat(deployment.getDeployedWorkflows())
        .extracting(Workflow::getWorkflowKey)
        .contains(2L, 3L);
    assertThat(deployment.getDeployedWorkflows())
        .extracting(Workflow::getResourceName)
        .contains("foo.bpmn", "bar.bpmn");
  }

  @Test
  public void shouldRejectCreateDeployment() {
    // given
    brokerRule.deployments().registerCreateCommand(b -> b.rejection());

    // then
    thrown.expect(ClientCommandRejectedException.class);
    thrown.expectMessage("Command (CREATE) was rejected");

    // when
    clientRule
        .workflowClient()
        .newDeployCommand()
        .addWorkflowModel(WORKFLOW_MODEL, "model.bpmn")
        .send()
        .join();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldDeployResourceAsWorkflowModel() {
    // given
    brokerRule.deployments().registerCreateCommand();

    // when
    clientRule
        .workflowClient()
        .newDeployCommand()
        .addWorkflowModel(WORKFLOW_MODEL, "model.bpmn")
        .send()
        .join();

    // then
    assertThat(brokerRule.getReceivedCommandRequests()).hasSize(1);

    final ExecuteCommandRequest commandRequest = brokerRule.getReceivedCommandRequests().get(0);
    final List<Map<String, Object>> resources =
        (List<Map<String, Object>>) commandRequest.getCommand().get("resources");
    assertThat(resources).hasSize(1);
    assertThat(resources.get(0))
        .containsEntry("resource", WORKFLOW_AS_BYTES)
        .containsEntry("resourceName", "model.bpmn")
        .containsEntry("resourceType", "BPMN_XML");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldDeployResourceAsString() {
    // given
    brokerRule.deployments().registerCreateCommand();

    // when
    clientRule
        .workflowClient()
        .newDeployCommand()
        .addResourceStringUtf8(Bpmn.convertToString(WORKFLOW_MODEL), "workflow.bpmn")
        .send()
        .join();

    // then
    assertThat(brokerRule.getReceivedCommandRequests()).hasSize(1);

    final ExecuteCommandRequest commandRequest = brokerRule.getReceivedCommandRequests().get(0);
    final List<Map<String, Object>> resources =
        (List<Map<String, Object>>) commandRequest.getCommand().get("resources");
    assertThat(resources).hasSize(1);
    assertThat(resources.get(0))
        .containsEntry("resource", WORKFLOW_AS_BYTES)
        .containsEntry("resourceName", "workflow.bpmn")
        .containsEntry("resourceType", "BPMN_XML");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldDeployResourceAsBytes() {
    // given
    brokerRule.deployments().registerCreateCommand();

    // when
    clientRule
        .workflowClient()
        .newDeployCommand()
        .addResourceBytes(WORKFLOW_AS_BYTES, "workflow.bpmn")
        .send()
        .join();

    // then
    assertThat(brokerRule.getReceivedCommandRequests()).hasSize(1);

    final ExecuteCommandRequest commandRequest = brokerRule.getReceivedCommandRequests().get(0);
    final List<Map<String, Object>> resources =
        (List<Map<String, Object>>) commandRequest.getCommand().get("resources");
    assertThat(resources).hasSize(1);
    assertThat(resources.get(0))
        .containsEntry("resource", WORKFLOW_AS_BYTES)
        .containsEntry("resourceName", "workflow.bpmn")
        .containsEntry("resourceType", "BPMN_XML");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldDeployResourceAsStream() {
    // given
    brokerRule.deployments().registerCreateCommand();

    // when
    clientRule
        .workflowClient()
        .newDeployCommand()
        .addResourceStream(new ByteArrayInputStream(WORKFLOW_AS_BYTES), "workflow.bpmn")
        .send()
        .join();

    // then
    assertThat(brokerRule.getReceivedCommandRequests()).hasSize(1);

    final ExecuteCommandRequest commandRequest = brokerRule.getReceivedCommandRequests().get(0);
    final List<Map<String, Object>> resources =
        (List<Map<String, Object>>) commandRequest.getCommand().get("resources");
    assertThat(resources).hasSize(1);
    assertThat(resources.get(0))
        .containsEntry("resource", WORKFLOW_AS_BYTES)
        .containsEntry("resourceName", "workflow.bpmn")
        .containsEntry("resourceType", "BPMN_XML");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldDeployResourceAsXmlFile() throws Exception {
    // given
    brokerRule.deployments().registerCreateCommand();

    // when
    final String filePath =
        getClass().getResource("/workflows/one-task-process.bpmn").toURI().getPath();

    clientRule.workflowClient().newDeployCommand().addResourceFile(filePath).send().join();

    // then
    assertThat(brokerRule.getReceivedCommandRequests()).hasSize(1);

    final ExecuteCommandRequest commandRequest = brokerRule.getReceivedCommandRequests().get(0);
    final List<Map<String, Object>> resources =
        (List<Map<String, Object>>) commandRequest.getCommand().get("resources");
    assertThat(resources).hasSize(1);
    assertThat(resources.get(0))
        .containsKey("resource")
        .containsEntry("resourceName", filePath)
        .containsEntry("resourceType", "BPMN_XML");

    final byte[] resource = (byte[]) resources.get(0).get("resource");
    assertThat(new File(filePath)).hasBinaryContent(resource);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldDeployResourceAsYamlFile() throws Exception {
    // given
    brokerRule.deployments().registerCreateCommand();

    // when
    final String filePath =
        getClass().getResource("/workflows/simple-workflow.yaml").toURI().getPath();

    clientRule.workflowClient().newDeployCommand().addResourceFile(filePath).send().join();

    // then
    assertThat(brokerRule.getReceivedCommandRequests()).hasSize(1);

    final ExecuteCommandRequest commandRequest = brokerRule.getReceivedCommandRequests().get(0);
    final List<Map<String, Object>> resources =
        (List<Map<String, Object>>) commandRequest.getCommand().get("resources");
    assertThat(resources).hasSize(1);
    assertThat(resources.get(0))
        .containsKey("resource")
        .containsEntry("resourceName", filePath)
        .containsEntry("resourceType", "YAML_WORKFLOW");

    final byte[] resource = (byte[]) resources.get(0).get("resource");
    assertThat(new File(filePath)).hasBinaryContent(resource);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldDeployResourceFromXmlClasspath() throws Exception {
    // given
    brokerRule.deployments().registerCreateCommand();

    // when
    clientRule
        .workflowClient()
        .newDeployCommand()
        .addResourceFromClasspath("workflows/one-task-process.bpmn")
        .send()
        .join();

    // then
    assertThat(brokerRule.getReceivedCommandRequests()).hasSize(1);

    final ExecuteCommandRequest commandRequest = brokerRule.getReceivedCommandRequests().get(0);
    final List<Map<String, Object>> resources =
        (List<Map<String, Object>>) commandRequest.getCommand().get("resources");
    assertThat(resources).hasSize(1);
    assertThat(resources.get(0))
        .containsKey("resource")
        .containsEntry("resourceName", "workflows/one-task-process.bpmn")
        .containsEntry("resourceType", "BPMN_XML");

    final byte[] resource = (byte[]) resources.get(0).get("resource");
    final String filePath =
        getClass().getResource("/workflows/one-task-process.bpmn").toURI().getPath();

    assertThat(new File(filePath)).hasBinaryContent(resource);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldDeployResourceFromYamlClasspath() throws Exception {
    // given
    brokerRule.deployments().registerCreateCommand();

    // when
    clientRule
        .workflowClient()
        .newDeployCommand()
        .addResourceFromClasspath("workflows/simple-workflow.yaml")
        .send()
        .join();

    // then
    assertThat(brokerRule.getReceivedCommandRequests()).hasSize(1);

    final ExecuteCommandRequest commandRequest = brokerRule.getReceivedCommandRequests().get(0);
    final List<Map<String, Object>> resources =
        (List<Map<String, Object>>) commandRequest.getCommand().get("resources");
    assertThat(resources).hasSize(1);
    assertThat(resources.get(0))
        .containsKey("resource")
        .containsEntry("resourceName", "workflows/simple-workflow.yaml")
        .containsEntry("resourceType", "YAML_WORKFLOW");

    final byte[] resource = (byte[]) resources.get(0).get("resource");
    final String filePath =
        getClass().getResource("/workflows/simple-workflow.yaml").toURI().getPath();

    assertThat(new File(filePath)).hasBinaryContent(resource);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldDeployMultipleResources() {
    // given
    brokerRule.deployments().registerCreateCommand();

    final BpmnModelInstance definition1 =
        Bpmn.createExecutableProcess("model1").startEvent().done();
    final BpmnModelInstance definition2 =
        Bpmn.createExecutableProcess("model2").startEvent().done();

    // when
    clientRule
        .workflowClient()
        .newDeployCommand()
        .addWorkflowModel(definition1, "model1.bpmn")
        .addWorkflowModel(definition2, "model2.bpmn")
        .send()
        .join();

    // then
    assertThat(brokerRule.getReceivedCommandRequests()).hasSize(1);

    final ExecuteCommandRequest commandRequest = brokerRule.getReceivedCommandRequests().get(0);
    final List<Map<String, Object>> resources =
        (List<Map<String, Object>>) commandRequest.getCommand().get("resources");
    assertThat(resources).hasSize(2);
    assertThat(resources).extracting("resourceName").contains("model1.bpmn", "model2.bpmn");
    assertThat(resources).extracting("resourceType").contains("BPMN_XML", "BPMN_XML");
    assertThat(resources)
        .extracting("resource")
        .contains(
            Bpmn.convertToString(definition1).getBytes(UTF_8),
            Bpmn.convertToString(definition2).getBytes(UTF_8));
  }

  @Test
  public void shouldBeNotValidIfResourceFileNotExist() {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Cannot deploy resource from file");

    clientRule.workflowClient().newDeployCommand().addResourceFile("not existing").send().join();
  }

  @Test
  public void shouldBeNotValidIfResourceClasspathFileNotExist() {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Cannot deploy resource from classpath");

    clientRule
        .workflowClient()
        .newDeployCommand()
        .addResourceFromClasspath("not existing")
        .send()
        .join();
  }

  @Test
  public void shouldBeNotValidIfResourceStreamNotExist() {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("resource stream must not be null");

    clientRule
        .workflowClient()
        .newDeployCommand()
        .addResourceStream(getClass().getResourceAsStream("not existing"), "workflow.bpmn")
        .send()
        .join();
  }

  @Test
  public void shouldBeNotValidIfResourceClasspathTypeIsUnknown() {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Cannot resolve type of resource 'example_file'.");

    clientRule
        .workflowClient()
        .newDeployCommand()
        .addResourceFromClasspath("example_file")
        .send()
        .join();
  }

  @Test
  public void shouldBeNotValidIfResourceFileTypeIsUnknown() throws Exception {
    final String filePath = getClass().getResource("/example_file").toURI().getPath();

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Cannot resolve type of resource");

    clientRule.workflowClient().newDeployCommand().addResourceFile(filePath).send().join();
  }
}
