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
package io.zeebe.workflow;

import io.zeebe.client.ZeebeClient;
import io.zeebe.client.api.ZeebeFuture;
import io.zeebe.client.api.clients.WorkflowClient;
import io.zeebe.client.api.events.DeploymentEvent;
import io.zeebe.client.api.events.WorkflowInstanceEvent;
import io.zeebe.client.cmd.ClientCommandRejectedException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

public class NonBlockingWorkflowInstanceStarter {
  public static void main(String[] args) {
    final String brokerContactPoint = "127.0.0.1:51015";
    final String bpmnProcessId = "demoProcess";
    final int partitionId = 0;

    final ZeebeClient zeebeClient =
        ZeebeClient.newClientBuilder()
            .brokerContactPoint(brokerContactPoint)
            .sendBufferSize(2)
            .build();

    final String topicName = zeebeClient.getConfiguration().getDefaultTopic();

    try {
      // try to create default topic if it not exists already
      zeebeClient
          .newCreateTopicCommand()
          .name(topicName)
          .partitions(4)
          .replicationFactor(1)
          .send()
          .join();
    } catch (final ClientCommandRejectedException e) {
      // topic already exists
    }

    System.out.println(String.format("> Connecting to %s", brokerContactPoint));

    System.out.println(
        String.format(
            "> Deploying workflow to topic '%s' and partition '%d'", topicName, partitionId));

    final WorkflowClient workflowClient = zeebeClient.topicClient(topicName).workflowClient();

    final DeploymentEvent deploymentResult =
        workflowClient
            .newDeployCommand()
            .addResourceFromClasspath("demoProcess.bpmn")
            .send()
            .join();

    try {
      final String deployedWorkflows =
          deploymentResult
              .getDeployedWorkflows()
              .stream()
              .map(wf -> String.format("<%s:%d>", wf.getBpmnProcessId(), wf.getVersion()))
              .collect(Collectors.joining(","));

      System.out.println(String.format("> Deployed: %s", deployedWorkflows));

      System.out.println(
          String.format("> Create workflow instance for workflow: %s", bpmnProcessId));

      final long time = System.currentTimeMillis();

      final AtomicInteger instancesCreated = new AtomicInteger();

      final ManyToOneConcurrentLinkedQueue<ZeebeFuture<WorkflowInstanceEvent>> requests =
          new ManyToOneConcurrentLinkedQueue();

      new Timer()
          .scheduleAtFixedRate(
              new TimerTask() {
                int errorCount = 0;

                @Override
                public void run() {
                  System.out.println(instancesCreated.getAndSet(0) + " " + errorCount);

                  while (requests.peek() != null) {
                    final ZeebeFuture<WorkflowInstanceEvent> future = requests.poll();

                    if (!future.isDone()) {
                      return;
                    }

                    try {
                      future.join();
                    } catch (Exception e) {
                      errorCount++;
                    }
                  }
                }
              },
              1_000,
              1_000);

      final long workflowKey = deploymentResult.getDeployedWorkflows().get(0).getWorkflowKey();

      for (int i = 0; i < 1_000_000; i++) {
        final ZeebeFuture<WorkflowInstanceEvent> future =
            workflowClient
                .newCreateInstanceCommand()
                .workflowKey(workflowKey)
                .payload("{\"a\": \"b\"}")
                .send();

        requests.add(future);

        instancesCreated.incrementAndGet();
      }

      System.out.println("Took: " + (System.currentTimeMillis() - time));
    } catch (ClientCommandRejectedException exception) {
      System.out.println(String.format("> Fail to deploy: %s", exception.getMessage()));
    }

    System.out.println("> Closing...");

    zeebeClient.close();

    System.out.println("> Closed.");
  }
}
