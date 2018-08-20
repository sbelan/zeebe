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
package io.zeebe.gateway;

import static io.zeebe.protocol.Protocol.DEFAULT_TOPIC;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import io.zeebe.gateway.api.events.DeploymentEvent;
import io.zeebe.gateway.protocol.GatewayOuterClass.DeployWorkflowRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.DeployWorkflowResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.Partition;
import io.zeebe.gateway.protocol.GatewayOuterClass.Partition.PartitionBrokerRole;
import io.zeebe.gateway.protocol.GatewayOuterClass.WorkflowInfoResponse;
import io.zeebe.gateway.util.RecordingStreamObserver;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import io.zeebe.util.sched.testing.ControlledActorSchedulerRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;

public class DeployWorkflowEndpointTest {

  private final DeployWorkflowRequest request = DeployWorkflowRequest.getDefaultInstance();
  private final RecordingStreamObserver<DeployWorkflowResponse> streamObserver =
      new RecordingStreamObserver<>();
  @Rule public ControlledActorSchedulerRule actorSchedulerRule = new ControlledActorSchedulerRule();
  @Mock private ResponseMapper responseMapper;
  @Mock private ClusterClient clusterClient;
  private EndpointManager endpointManager;
  private DeployWorkflowResponse response;

  @Before
  public void setUp() {
    initMocks(this);

    endpointManager = new EndpointManager(responseMapper, clusterClient, actorSchedulerRule.get());

    final Partition partition =
        Partition.newBuilder()
            .setPartitionId(5)
            .setRole(PartitionBrokerRole.LEADER)
            .setTopicName(DEFAULT_TOPIC)
            .build();

    this.response =
        DeployWorkflowResponse.newBuilder()
            .addWorkflows(
                WorkflowInfoResponse.newBuilder()
                    .setVersion(5)
                    .setWorkflowKey(123456789L)
                    .setBpmnProcessId("demoProcess")
                    .setResourceName("demo-process")
                    .build())
            .build();

    when(responseMapper.toDeployWorkflowResponse(any())).thenReturn(response);
  }

  @Test
  public void deployWorkflowShouldCheckCorrectInvocation() {
    // given
    final ActorFuture<DeploymentEvent> responseFuture = CompletableActorFuture.completed(null);
    when(clusterClient.sendDeployWorkflowRequest(any())).thenReturn(responseFuture);

    // when
    sendRequest();

    // then
    streamObserver.assertValues(response);
  }

  @Test
  public void healthCheckShouldProduceException() {
    // given
    final RuntimeException exception = new RuntimeException("test");
    when(clusterClient.sendDeployWorkflowRequest(any()))
        .thenReturn(CompletableActorFuture.completedExceptionally(exception));

    // when
    sendRequest();

    // then
    streamObserver.assertErrors(exception);
  }

  private void sendRequest() {
    endpointManager.deployWorkflow(this.request, streamObserver);
    actorSchedulerRule.workUntilDone();
  }
}
