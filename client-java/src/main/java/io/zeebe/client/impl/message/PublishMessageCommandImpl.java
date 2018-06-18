/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.zeebe.client.impl.message;

import io.zeebe.client.api.commands.*;
import io.zeebe.client.api.commands.PublishMessageCommandStep1.PublishMessageCommandStep2;
import io.zeebe.client.api.commands.PublishMessageCommandStep1.PublishMessageCommandStep3;
import io.zeebe.client.api.events.MessageEvent;
import io.zeebe.client.impl.*;
import io.zeebe.client.impl.RequestManager.ResponseFuture;
import io.zeebe.client.impl.command.MessageCommandImpl;
import io.zeebe.client.impl.data.ZeebeObjectMapperImpl;
import io.zeebe.client.impl.record.RecordImpl;
import io.zeebe.protocol.intent.MessageIntent;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class PublishMessageCommandImpl extends CommandImpl<MessageEvent> implements
        PublishMessageCommandStep1, PublishMessageCommandStep2, PublishMessageCommandStep3 {

    private final MessageCommandImpl command;
    private final ClientTopicsMetada topicsMetadata;
    private final String topic;

    public PublishMessageCommandImpl(RequestManager commandManager,
            ZeebeObjectMapperImpl objectMapper, String topic, ClientTopicsMetada topicsMetadata) {
        super(commandManager);
        this.topic = topic;
        this.topicsMetadata = topicsMetadata;

        command = new MessageCommandImpl(objectMapper, MessageIntent.PUBLISH);

        command.setTopicName(topic);
    }

    @Override
    public RecordImpl getCommand() {
        return command;
    }

    @Override
    public PublishMessageCommandStep3 payload(InputStream payload) {
        command.setPayload(payload);
        return this;
    }

    @Override
    public PublishMessageCommandStep3 payload(String payload) {
        command.setPayload(payload);
        return this;
    }

    @Override
    public PublishMessageCommandStep3 payload(Map<String, Object> payload) {
        command.setPayload(payload);
        return this;
    }

    @Override
    public PublishMessageCommandStep3 messageKey(String key) {
        command.setMessageKey(key);
        return this;
    }

    @Override
    public PublishMessageCommandStep2 messageName(String messageName) {
        command.setMessageName(messageName);
        return this;
    }

    @Override
    public ResponseFuture<MessageEvent> send() {

        final long deadline = System.currentTimeMillis() + 15_000; // TODO: use timeout
        final Topic topicMetadata = topicsMetadata.getTopic(topic, deadline);

        if (topicMetadata == null)
        {
            final String errorMessage = String.format("Cannot publish message to topic '%s', no such topic exists.", topic);
            throw new RuntimeException(errorMessage);
        }
        else
        {
            final String messageKey = command.getMessageKey(); // TODO null check
            MessageDigest digest = getHashProvider();
            digest.update(messageKey.getBytes(StandardCharsets.UTF_8));

            final byte[] hashBytes = digest.digest();
            final int hash = Math.abs(Arrays.hashCode(hashBytes));

            final List<Partition> partitions = topicMetadata.getPartitions();
            final int offset = hash % partitions.size();

            command.setPartitionId(partitions.get(offset).getId());

            return super.send();
        }
    }

    private MessageDigest getHashProvider() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
