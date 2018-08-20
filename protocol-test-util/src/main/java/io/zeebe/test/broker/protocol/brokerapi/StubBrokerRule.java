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
package io.zeebe.test.broker.protocol.brokerapi;

import static io.zeebe.protocol.Protocol.DEFAULT_TOPIC;
import static io.zeebe.protocol.Protocol.DEPLOYMENT_PARTITION;

import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.clientapi.ControlMessageType;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.SubscriptionType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.Intent;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.protocol.intent.SubscriberIntent;
import io.zeebe.protocol.intent.SubscriptionIntent;
import io.zeebe.test.broker.protocol.MsgPackHelper;
import io.zeebe.test.broker.protocol.brokerapi.data.BrokerPartitionState;
import io.zeebe.test.broker.protocol.brokerapi.data.Topology;
import io.zeebe.test.broker.protocol.brokerapi.data.TopologyBroker;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.ServerTransport;
import io.zeebe.transport.SocketAddress;
import io.zeebe.transport.Transports;
import io.zeebe.transport.impl.util.SocketUtil;
import io.zeebe.util.sched.ActorScheduler;
import io.zeebe.util.sched.clock.ControlledActorClock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.junit.rules.ExternalResource;

public class StubBrokerRule extends ExternalResource {
  public static final String TEST_TOPIC_NAME = DEFAULT_TOPIC;
  public static final int TEST_PARTITION_ID = DEPLOYMENT_PARTITION;

  private ControlledActorClock clock = new ControlledActorClock();
  protected ActorScheduler scheduler;

  protected final SocketAddress socketAddress;

  protected ServerTransport transport;

  protected StubResponseChannelHandler channelHandler;
  protected MsgPackHelper msgPackHelper;

  protected AtomicReference<Topology> currentTopology = new AtomicReference<>();

  private final int partitionCount;

  public StubBrokerRule() {
    this(1);
  }

  public StubBrokerRule(int partitionCount) {
    this.socketAddress = SocketUtil.getNextAddress();
    this.partitionCount = partitionCount;
  }

  @Override
  protected void before() {
    msgPackHelper = new MsgPackHelper();

    final int numThreads = 2;
    scheduler =
        ActorScheduler.newActorScheduler()
            .setCpuBoundActorThreadCount(numThreads)
            .setActorClock(clock)
            .build();

    scheduler.start();

    channelHandler = new StubResponseChannelHandler(msgPackHelper);

    final Topology topology = new Topology();
    topology.addLeader(socketAddress, Protocol.SYSTEM_TOPIC, Protocol.SYSTEM_PARTITION);

    for (int i = TEST_PARTITION_ID; i < TEST_PARTITION_ID + partitionCount; i++) {
      topology.addLeader(socketAddress, DEFAULT_TOPIC, i);
    }

    currentTopology.set(topology);
    stubTopologyRequest();
    bindTransport();
  }

  @Override
  protected void after() {
    if (transport != null) {
      closeTransport();
    }
    if (scheduler != null) {
      scheduler.stop();
    }
  }

  public void interruptAllServerChannels() {
    transport.interruptAllChannels();
  }

  public void closeTransport() {
    if (transport != null) {
      transport.close();
      transport = null;
    } else {
      throw new RuntimeException("transport not open");
    }
  }

  public void bindTransport() {
    if (transport == null) {
      transport =
          Transports.newServerTransport()
              .bindAddress(socketAddress.toInetSocketAddress())
              .scheduler(scheduler)
              .build(null, channelHandler);
    } else {
      throw new RuntimeException("transport already open");
    }
  }

  public ServerTransport getTransport() {
    return transport;
  }

  public ExecuteCommandResponseTypeBuilder onExecuteCommandRequest() {
    return onExecuteCommandRequest((r) -> true);
  }

  public ExecuteCommandResponseTypeBuilder onExecuteCommandRequest(
      Predicate<ExecuteCommandRequest> activationFunction) {
    return new ExecuteCommandResponseTypeBuilder(
        channelHandler::addExecuteCommandRequestStub, activationFunction, msgPackHelper);
  }

  public ExecuteCommandResponseTypeBuilder onExecuteCommandRequest(
      ValueType eventType, Intent intent) {
    return onExecuteCommandRequest(ecr -> ecr.valueType() == eventType && ecr.intent() == intent);
  }

  public ExecuteCommandResponseTypeBuilder onExecuteCommandRequest(
      int partitionId, ValueType valueType, Intent intent) {
    return onExecuteCommandRequest(
        ecr ->
            ecr.partitionId() == partitionId
                && ecr.valueType() == valueType
                && ecr.intent() == intent);
  }

  public ControlMessageResponseTypeBuilder onControlMessageRequest() {
    return onControlMessageRequest((r) -> true);
  }

  public ControlMessageResponseTypeBuilder onControlMessageRequest(
      Predicate<ControlMessageRequest> activationFunction) {
    return new ControlMessageResponseTypeBuilder(
        channelHandler::addControlMessageRequestStub, activationFunction, msgPackHelper);
  }

  public List<ControlMessageRequest> getReceivedControlMessageRequests() {
    return channelHandler.getReceivedControlMessageRequests();
  }

  public List<ControlMessageRequest> getReceivedControlMessageRequestsByType(
      ControlMessageType type) {
    return channelHandler
        .getReceivedControlMessageRequests()
        .stream()
        .filter((r) -> type == r.messageType())
        .collect(Collectors.toList());
  }

  public List<ExecuteCommandRequest> getReceivedCommandRequests() {
    return channelHandler.getReceivedCommandRequests();
  }

  public List<Object> getAllReceivedRequests() {
    return channelHandler.getAllReceivedRequests();
  }

  public SubscribedRecordBuilder newSubscribedEvent() {
    return new SubscribedRecordBuilder(msgPackHelper, transport);
  }

  public void stubTopologyRequest() {
    onTopologyRequest()
        .respondWith()
        .data()
        .put("brokers", r -> currentTopology.get().getBrokers())
        .done()
        .register();

    // assuming that topology and partitions request are consistent
    onControlMessageRequest(
            r ->
                r.messageType() == ControlMessageType.REQUEST_PARTITIONS
                    && r.partitionId() == Protocol.SYSTEM_PARTITION)
        .respondWith()
        .data()
        .put(
            "partitions",
            r -> {
              final Topology topology = currentTopology.get();
              final List<Map<String, Object>> partitions = new ArrayList<>();
              for (TopologyBroker broker : topology.getBrokers()) {
                for (BrokerPartitionState brokerPartitionState : broker.getPartitions()) {
                  final Map<String, Object> partition = new HashMap<>();
                  partition.put("topic", brokerPartitionState.getTopicName());
                  partition.put("id", brokerPartitionState.getPartitionId());
                  partitions.add(partition);
                }
              }
              return partitions;
            })
        .done()
        .register();
  }

  public ControlMessageResponseTypeBuilder onTopologyRequest() {
    return onControlMessageRequest(r -> r.messageType() == ControlMessageType.REQUEST_TOPOLOGY);
  }

  public void addTopic(String topic, int partition) {
    final Topology newTopology = new Topology(currentTopology.get());

    newTopology.addLeader(socketAddress, topic, partition);
    currentTopology.set(newTopology);
  }

  public void addSystemTopic() {
    addTopic(Protocol.SYSTEM_TOPIC, Protocol.SYSTEM_PARTITION);
  }

  public void setCurrentTopology(Topology currentTopology) {
    this.currentTopology.set(currentTopology);
  }

  public void clearTopology() {
    currentTopology.set(new Topology());
  }

  public void stubTopicSubscriptionApi(long initialSubscriberKey) {
    final AtomicLong subscriberKeyProvider = new AtomicLong(initialSubscriberKey);
    final AtomicLong subscriptionKeyProvider = new AtomicLong(0);

    onExecuteCommandRequest(ValueType.SUBSCRIBER, SubscriberIntent.SUBSCRIBE)
        .respondWith()
        .event()
        .intent(SubscriberIntent.SUBSCRIBED)
        .key((r) -> subscriberKeyProvider.getAndIncrement())
        .value()
        .allOf((r) -> r.getCommand())
        .done()
        .register();

    onControlMessageRequest((r) -> r.messageType() == ControlMessageType.REMOVE_TOPIC_SUBSCRIPTION)
        .respondWith()
        .data()
        .allOf((r) -> r.getData())
        .done()
        .register();

    onExecuteCommandRequest(ValueType.SUBSCRIPTION, SubscriptionIntent.ACKNOWLEDGE)
        .respondWith()
        .event()
        .intent(SubscriptionIntent.ACKNOWLEDGED)
        .key((r) -> subscriptionKeyProvider.getAndIncrement())
        .partitionId((r) -> r.partitionId())
        .value()
        .allOf((r) -> r.getCommand())
        .done()
        .register();
  }

  public void stubJobSubscriptionApi(long initialSubscriberKey) {
    final AtomicLong subscriberKeyProvider = new AtomicLong(initialSubscriberKey);

    onControlMessageRequest((r) -> r.messageType() == ControlMessageType.ADD_JOB_SUBSCRIPTION)
        .respondWith()
        .data()
        .allOf((r) -> r.getData())
        .put("subscriberKey", (r) -> subscriberKeyProvider.getAndIncrement())
        .done()
        .register();

    onControlMessageRequest((r) -> r.messageType() == ControlMessageType.REMOVE_JOB_SUBSCRIPTION)
        .respondWith()
        .data()
        .allOf((r) -> r.getData())
        .done()
        .register();

    onControlMessageRequest(
            (r) -> r.messageType() == ControlMessageType.INCREASE_JOB_SUBSCRIPTION_CREDITS)
        .respondWith()
        .data()
        .allOf((r) -> r.getData())
        .done()
        .register();
  }

  public void pushRaftEvent(RemoteAddress remote, long subscriberKey, long key, long position) {
    pushRecord(
        remote, subscriberKey, key, position, RecordType.EVENT, ValueType.RAFT, Intent.UNKNOWN);
  }

  public void pushRecord(
      RemoteAddress remote,
      long subscriberKey,
      long key,
      long position,
      RecordType recordType,
      ValueType valueType,
      Intent intent) {
    pushRecord(
        remote,
        subscriberKey,
        key,
        position,
        clock.getCurrentTime(),
        recordType,
        valueType,
        intent);
  }

  public void pushRecord(
      RemoteAddress remote,
      long subscriberKey,
      long key,
      long position,
      Instant timestamp,
      RecordType recordType,
      ValueType valueType,
      Intent intent) {
    newSubscribedEvent()
        .partitionId(TEST_PARTITION_ID)
        .key(key)
        .sourceRecordPosition(position - 1L)
        .position(position)
        .recordType(recordType)
        .valueType(valueType)
        .intent(intent)
        .subscriberKey(subscriberKey)
        .subscriptionType(SubscriptionType.TOPIC_SUBSCRIPTION)
        .timestamp(timestamp)
        .value()
        .done()
        .push(remote);
  }

  public void pushTopicEvent(RemoteAddress remote, Consumer<SubscribedRecordBuilder> eventConfig) {
    final SubscribedRecordBuilder builder =
        newSubscribedEvent().subscriptionType(SubscriptionType.TOPIC_SUBSCRIPTION);

    // defaults that the config can override
    builder.recordType(RecordType.EVENT);
    builder.intent(Intent.UNKNOWN);
    builder.key(0);
    builder.position(0);
    builder.valueType(ValueType.RAFT);
    builder.subscriberKey(0);
    builder.timestamp(clock.getCurrentTime());
    builder.partitionId(TEST_PARTITION_ID);
    builder.value().done();

    eventConfig.accept(builder);

    builder.push(remote);
  }

  public void pushActivatedJob(
      RemoteAddress remote,
      long subscriberKey,
      long key,
      long position,
      String worker,
      String jobType) {
    newSubscribedEvent()
        .partitionId(TEST_PARTITION_ID)
        .key(key)
        .sourceRecordPosition(position - 1)
        .position(position)
        .recordType(RecordType.EVENT)
        .valueType(ValueType.JOB)
        .intent(JobIntent.ACTIVATED)
        .subscriberKey(subscriberKey)
        .subscriptionType(SubscriptionType.JOB_SUBSCRIPTION)
        .timestamp(clock.getCurrentTime())
        .value()
        .put("type", jobType)
        .put("deadline", 1000L)
        .put("worker", worker)
        .put("retries", 3)
        .put("payload", msgPackHelper.encodeAsMsgPack(new HashMap<>()))
        .put("state", "LOCKED")
        .done()
        .push(remote);
  }

  public JobStubs jobs() {
    return new JobStubs(this);
  }

  public WorkflowInstanceStubs workflowInstances() {
    return new WorkflowInstanceStubs(this);
  }

  public DeploymentStubs deployments() {
    return new DeploymentStubs(this);
  }

  public SocketAddress getSocketAddress() {
    return socketAddress;
  }

  public ControlledActorClock getClock() {
    return clock;
  }
}
