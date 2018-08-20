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
package io.zeebe.transport;

import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.zeebe.dispatcher.Dispatcher;
import io.zeebe.dispatcher.Dispatchers;
import io.zeebe.test.util.AutoCloseableRule;
import io.zeebe.transport.impl.memory.NonBlockingMemoryPool;
import io.zeebe.transport.impl.util.SocketUtil;
import io.zeebe.transport.util.ControllableServerTransport;
import io.zeebe.transport.util.EchoRequestResponseHandler;
import io.zeebe.transport.util.RecordingMessageHandler;
import io.zeebe.util.ByteValue;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.buffer.BufferWriter;
import io.zeebe.util.buffer.DirectBufferWriter;
import io.zeebe.util.sched.clock.ControlledActorClock;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.testing.ActorSchedulerRule;
import java.time.Duration;
import java.util.function.Function;
import org.agrona.DirectBuffer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public class ClientTransportMemoryTest {
  private ControlledActorClock clock = new ControlledActorClock();
  public ActorSchedulerRule actorSchedulerRule = new ActorSchedulerRule(3, clock);
  public AutoCloseableRule closeables = new AutoCloseableRule();

  @Rule public RuleChain ruleChain = RuleChain.outerRule(actorSchedulerRule).around(closeables);

  public static final DirectBuffer BUF1 = BufferUtil.wrapBytes(1, 2, 3, 4);
  public static final SocketAddress SERVER_ADDRESS1 = SocketUtil.getNextAddress();

  protected ClientTransport clientTransport;

  private NonBlockingMemoryPool requestMemoryPoolSpy;
  private NonBlockingMemoryPool messageMemoryPoolSpy;

  @Before
  public void setUp() {
    requestMemoryPoolSpy = spy(new NonBlockingMemoryPool(ByteValue.ofMegabytes(4)));
    messageMemoryPoolSpy = spy(new NonBlockingMemoryPool(ByteValue.ofMegabytes(4)));

    clientTransport =
        Transports.newClientTransport()
            .scheduler(actorSchedulerRule.get())
            .requestMemoryPool(requestMemoryPoolSpy)
            .messageMemoryPool(messageMemoryPoolSpy)
            .defaultMessageRetryTimeout(Duration.ofMillis(100))
            .build();
    closeables.manage(clientTransport);
  }

  protected ControllableServerTransport buildControllableServerTransport() {
    final ControllableServerTransport serverTransport = new ControllableServerTransport();
    closeables.manage(serverTransport);
    return serverTransport;
  }

  protected ServerTransport buildServerTransport(
      Function<ServerTransportBuilder, ServerTransport> builderConsumer) {
    final Dispatcher serverSendBuffer =
        Dispatchers.create("serverSendBuffer")
            .bufferSize(ByteValue.ofMegabytes(4))
            .actorScheduler(actorSchedulerRule.get())
            .build();
    closeables.manage(serverSendBuffer);

    final ServerTransportBuilder transportBuilder =
        Transports.newServerTransport().scheduler(actorSchedulerRule.get());

    final ServerTransport serverTransport = builderConsumer.apply(transportBuilder);
    closeables.manage(serverTransport);

    return serverTransport;
  }

  @Test
  public void shouldReclaimRequestMemoryOnRequestTimeout() {
    // given
    final RemoteAddress remote = clientTransport.registerRemoteAddress(SERVER_ADDRESS1);

    final ClientOutput output = clientTransport.getOutput();

    // when
    final ActorFuture<ClientResponse> responseFuture =
        output.sendRequest(remote, new DirectBufferWriter().wrap(BUF1), Duration.ofMillis(500));

    // then
    assertThatThrownBy(() -> responseFuture.join())
        .hasMessageContaining("Request timed out after PT0.5S");
    verify(requestMemoryPoolSpy, times(1)).allocate(anyInt());
    verify(requestMemoryPoolSpy, timeout(500).times(1))
        .reclaim(any()); // released after future is completed
  }

  @Test
  public void shouldReclaimRequestMemoryIfFutureCompleteFails() {
    // given
    final RemoteAddress remote = clientTransport.registerRemoteAddress(SERVER_ADDRESS1);
    final ClientOutput output = clientTransport.getOutput();
    final ActorFuture<ClientResponse> responseFuture =
        output.sendRequest(remote, new DirectBufferWriter().wrap(BUF1), Duration.ofMillis(500));

    // when

    // future is completed before timeout occurs to simmulate situation in which future can not be
    // completed
    responseFuture.complete(null);

    // then
    verify(requestMemoryPoolSpy, times(1)).allocate(anyInt());
    verify(requestMemoryPoolSpy, timeout(5000).times(1)).reclaim(any()); // released of timeout
  }

  @Test
  public void shouldReclaimRequestMemoryOnResponse() throws InterruptedException {
    // given
    final BufferWriter writer = mock(BufferWriter.class);
    when(writer.getLength()).thenReturn(16);

    buildServerTransport(
        b ->
            b.bindAddress(SERVER_ADDRESS1.toInetSocketAddress())
                .build(null, new EchoRequestResponseHandler()));

    final RemoteAddress remote = clientTransport.registerRemoteAddress(SERVER_ADDRESS1);

    clientTransport.getOutput().sendRequest(remote, writer).join();

    verify(requestMemoryPoolSpy, times(1)).allocate(anyInt());
    verify(requestMemoryPoolSpy, timeout(500).times(1))
        .reclaim(any()); // released after future is completed
  }

  @Test
  public void shouldReclaimRequestMemoryOnRequestWriterException() throws InterruptedException {
    // given
    final BufferWriter writer = mock(BufferWriter.class);
    when(writer.getLength()).thenReturn(16);
    doThrow(RuntimeException.class).when(writer).write(any(), anyInt());

    final RemoteAddress remote = clientTransport.registerRemoteAddress(SERVER_ADDRESS1);

    try {
      clientTransport.getOutput().sendRequest(remote, writer);
    } catch (RuntimeException e) {
      // expected
    }

    verify(requestMemoryPoolSpy, times(1)).allocate(anyInt());
    verify(requestMemoryPoolSpy, times(1)).reclaim(any());
  }

  @Test
  public void shouldReclaimOnMessageSend() throws InterruptedException {
    // given
    final BufferWriter writer = mock(BufferWriter.class);
    when(writer.getLength()).thenReturn(16);

    final RecordingMessageHandler messageHandler = new RecordingMessageHandler();

    buildServerTransport(
        b -> {
          return b.bindAddress(SERVER_ADDRESS1.toInetSocketAddress()).build(messageHandler, null);
        });

    final RemoteAddress remote = clientTransport.registerRemoteAndAwaitChannel(SERVER_ADDRESS1);

    final TransportMessage message = new TransportMessage();

    message.writer(writer);
    message.remoteAddress(remote);

    clientTransport.getOutput().sendMessage(message);

    waitUntil(() -> messageHandler.numReceivedMessages() == 1);

    verify(messageMemoryPoolSpy, times(1)).allocate(anyInt());
    verify(messageMemoryPoolSpy, times(1)).reclaim(any());
  }

  @Test
  public void shouldReclaimOnMessageSendFailed() throws InterruptedException {
    // given
    final BufferWriter writer = mock(BufferWriter.class);
    when(writer.getLength()).thenReturn(16);

    // no channel open
    final RemoteAddress remote = clientTransport.registerRemoteAddress(SERVER_ADDRESS1);

    final TransportMessage message = new TransportMessage();

    message.writer(writer);
    message.remoteAddress(remote);

    // when
    clientTransport.getOutput().sendMessage(message);

    // then
    verify(messageMemoryPoolSpy, times(1)).allocate(anyInt());
    verify(messageMemoryPoolSpy, timeout(1000).times(1)).reclaim(any());
  }

  @Test
  public void shouldReclaimOnMessageWriterException() throws InterruptedException {
    // given
    final BufferWriter writer = mock(BufferWriter.class);
    when(writer.getLength()).thenReturn(16);
    doThrow(RuntimeException.class).when(writer).write(any(), anyInt());

    final RemoteAddress remote = clientTransport.registerRemoteAddress(SERVER_ADDRESS1);

    final TransportMessage message = new TransportMessage();

    message.writer(writer);
    message.remoteAddress(remote);

    try {
      clientTransport.getOutput().sendMessage(message);
      fail("expected exception");
    } catch (Exception e) {
      // expected
    }

    verify(messageMemoryPoolSpy, times(1)).allocate(anyInt());
    verify(messageMemoryPoolSpy, times(1)).reclaim(any());
  }

  @Test
  public void shouldRejectMessageWhenBufferPoolExhaused() {
    // given
    final ClientOutput output = clientTransport.getOutput();
    final TransportMessage message = new TransportMessage().buffer(BUF1).remoteStreamId(0);

    doReturn(null).when(messageMemoryPoolSpy).allocate(anyInt());

    // when
    final boolean success = output.sendMessage(message);

    // then
    assertThat(success).isFalse();
  }

  @Test
  public void shouldRejectRequestWhenBufferPoolExhaused() {
    // given
    final ClientOutput output = clientTransport.getOutput();
    final RemoteAddress addr = clientTransport.registerRemoteAddress(SERVER_ADDRESS1);

    doReturn(null).when(requestMemoryPoolSpy).allocate(anyInt());

    // when
    final ActorFuture<ClientResponse> reqFuture =
        output.sendRequest(addr, new DirectBufferWriter().wrap(BUF1));

    // then
    assertThat(reqFuture).isNull();
  }
}
