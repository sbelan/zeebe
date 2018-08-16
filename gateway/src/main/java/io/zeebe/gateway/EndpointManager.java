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

import io.grpc.stub.StreamObserver;
import io.zeebe.gateway.api.commands.Partition;
import io.zeebe.gateway.api.commands.Topic;
import io.zeebe.gateway.api.commands.Topics;
import io.zeebe.gateway.api.commands.Topology;
import io.zeebe.gateway.api.events.JobActivateEvent;
import io.zeebe.gateway.api.events.JobEvent;
import io.zeebe.gateway.protocol.GatewayGrpc;
import io.zeebe.gateway.protocol.GatewayOuterClass.ActivateJobRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.ActivateJobResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.HealthRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.HealthResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.JobInfo;
import io.zeebe.util.EnsureUtil;
import java.util.List;
import java.util.Optional;

public class EndpointManager extends GatewayGrpc.GatewayImplBase {

  private final ResponseMapper responseMapper;
  private final ZeebeClient zbClient;

  public ResponseMapper getResponseMapper() {
    return responseMapper;
  }

  public ZeebeClient getZbClient() {
    return zbClient;
  }

  EndpointManager(final ResponseMapper mapper, final ZeebeClient clusterClient) {
    this.responseMapper = mapper;
    this.zbClient = clusterClient;
  }

  @Override
  public void health(
      final HealthRequest request, final StreamObserver<HealthResponse> responseObserver) {

    try {
      final Topology response = zbClient.newTopologyRequest().send().join();

      responseObserver.onNext(responseMapper.toResponse(response));
      responseObserver.onCompleted();

    } catch (final RuntimeException e) {
      responseObserver.onError(e);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void activateJobs(
      ActivateJobRequest request, StreamObserver<ActivateJobResponse> responseObserver) {
    try {
      final Topics topicsResponse = zbClient.newTopicsRequest().send().join();
      final List<Topic> topics = topicsResponse.getTopics();

      EnsureUtil.ensureNotNull("topics", topics);

      final Optional<Topic> topic =
          topics.stream().filter(t -> t.getName().equals(request.getTopicName())).findFirst();

      EnsureUtil.ensurePresent("topic", topic);

      final List<Partition> partitions = topic.get().getPartitions();
      final ActivateJobResponse.Builder response = ActivateJobResponse.newBuilder();

      for (final Partition partition : partitions) {
        final JobActivateEvent activatedEvent =
            zbClient
                .topicClient()
                .jobClient()
                .newActivateJobsCommand()
                .amount(request.getAmount())
                .jobType(request.getType())
                .workerName(request.getWorker())
                .send()
                .join();

        final List<JobEvent> activatedJobs = activatedEvent.getJobs();
        if (activatedJobs != null) {
          for (final JobEvent activatedJob : activatedJobs) {
            response.addJobs(JobInfo.newBuilder().setKey(activatedJob.getKey()).build());
            if (response.getJobsCount() >= request.getAmount()) {
              break;
            }
          }
        }

        if (response.getJobsCount() >= request.getAmount()) {
          break;
        }
      }

      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    } catch (final RuntimeException e) {
      responseObserver.onError(e);
      responseObserver.onCompleted();
    }
  }
}
