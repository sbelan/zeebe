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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.zeebe.test.util.AutoCloseableRule;
import io.zeebe.transport.impl.memory.TransportMemoryPool;
import io.zeebe.transport.impl.util.SocketUtil;
import io.zeebe.util.buffer.BufferWriter;
import io.zeebe.util.sched.testing.ActorSchedulerRule;
import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public class ServerTransportMemoryPoolTest {
  protected static final SocketAddress ADDRESS = SocketUtil.getNextAddress();

  public static final DirectBuffer BUF1 = new UnsafeBuffer(new byte[32]);

  public ActorSchedulerRule actorSchedulerRule = new ActorSchedulerRule();
  public AutoCloseableRule closeables = new AutoCloseableRule();

  @Rule public RuleChain ruleChain = RuleChain.outerRule(actorSchedulerRule).around(closeables);

  protected ServerTransport serverTransport;

  private TransportMemoryPool messageMemoryPool;

  @Before
  public void setUp() {
    messageMemoryPool = mock(TransportMemoryPool.class);

    serverTransport =
        Transports.newServerTransport()
            .bindAddress(ADDRESS.toInetSocketAddress())
            .scheduler(actorSchedulerRule.get())
            .messageMemoryPool(messageMemoryPool)
            .build(null, null);
  }

  @After
  public void tearDown() {
    serverTransport.close();
  }

  @Test
  public void shouldRejectMessageWhenBufferPoolExhaused() {
    // given
    final ServerOutput output = serverTransport.getOutput();
    final TransportMessage message = new TransportMessage().buffer(BUF1).remoteStreamId(0);

    doReturn(null).when(messageMemoryPool).allocate(anyInt());

    // when
    final boolean success = output.sendMessage(message);

    // then
    assertThat(success).isFalse();
  }

  @Test
  public void shouldRejectResponseWhenBufferPoolExhaused() {
    // given
    final ServerOutput output = serverTransport.getOutput();
    final ServerResponse response =
        new ServerResponse().buffer(BUF1).remoteStreamId(0).requestId(1L);

    doReturn(null).when(messageMemoryPool).allocate(anyInt());

    // when
    final boolean success = output.sendResponse(response);

    // then
    assertThat(success).isFalse();
  }

  @Test
  public void shouldReclaimOnResponseWriterException() throws InterruptedException {
    final ServerOutput output = serverTransport.getOutput();

    // given
    final BufferWriter writer = mock(BufferWriter.class);
    when(writer.getLength()).thenReturn(16);
    doThrow(RuntimeException.class).when(writer).write(any(), anyInt());

    final ServerResponse response =
        new ServerResponse().writer(writer).remoteStreamId(0).requestId(1L);

    doReturn(ByteBuffer.allocate(50)).when(messageMemoryPool).allocate(anyInt());

    // when
    try {
      output.sendResponse(response);
      fail("expected exception");
    } catch (Exception e) {
      // expected
    }

    // then

    verify(messageMemoryPool, times(1)).allocate(anyInt());
    verify(messageMemoryPool, times(1)).reclaim(any());
  }

  @Test
  public void shouldReclaimOnMessageWriterException() throws InterruptedException {
    final ServerOutput output = serverTransport.getOutput();

    // given
    final BufferWriter writer = mock(BufferWriter.class);
    when(writer.getLength()).thenReturn(16);
    doThrow(RuntimeException.class).when(writer).write(any(), anyInt());

    final TransportMessage message = new TransportMessage().writer(writer).remoteStreamId(0);

    doReturn(ByteBuffer.allocate(50)).when(messageMemoryPool).allocate(anyInt());

    // when
    try {
      output.sendMessage(message);
      fail("expected exception");
    } catch (Exception e) {
      // expected
    }

    // then

    verify(messageMemoryPool, times(1)).allocate(anyInt());
    verify(messageMemoryPool, times(1)).reclaim(any());
  }
}
