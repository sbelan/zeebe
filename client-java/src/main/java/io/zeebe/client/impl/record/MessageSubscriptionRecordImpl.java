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
package io.zeebe.client.impl.record;

import io.zeebe.client.impl.data.ZeebeObjectMapperImpl;
import io.zeebe.client.impl.event.MessageSubscriptionEventImpl;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.MessageIntent;

public abstract class MessageSubscriptionRecordImpl extends RecordImpl
{

    private String messageName;
    private String messageKey;
    private int partitionId;
    private long workflowInstanceKey;
    private long activityInstanceId;

    public MessageSubscriptionRecordImpl(ZeebeObjectMapperImpl objectMapper, RecordType recordType)
    {
        super(objectMapper, recordType, ValueType.MESSAGE_SUBSCRIPTION);
    }

    public MessageSubscriptionRecordImpl(MessageSubscriptionRecordImpl base, MessageIntent intent)
    {
        super(base, intent);

        this.messageName = base.messageName;
        this.messageKey = base.messageKey;
        this.partitionId = base.partitionId;
        this.workflowInstanceKey = base.workflowInstanceKey;
        this.activityInstanceId = base.activityInstanceId;
    }

    public String getMessageName()
    {
        return messageName;
    }

    public void setMessageName(String messageName)
    {
        this.messageName = messageName;
    }

    public String getMessageKey()
    {
        return messageKey;
    }

    public void setMessageKey(String messageKey)
    {
        this.messageKey = messageKey;
    }

    public int getPartitionId()
    {
        return partitionId;
    }

    @Override
    public void setPartitionId(int partitionId)
    {
        this.partitionId = partitionId;
    }

    public long getWorkflowInstanceKey()
    {
        return workflowInstanceKey;
    }

    public void setWorkflowInstanceKey(long workflowInstanceKey)
    {
        this.workflowInstanceKey = workflowInstanceKey;
    }

    public long getActivityInstanceId()
    {
        return activityInstanceId;
    }

    public void setActivityInstanceId(long activityInstanceId)
    {
        this.activityInstanceId = activityInstanceId;
    }

    @Override
    public Class<? extends RecordImpl> getEventClass()
    {
        return MessageSubscriptionEventImpl.class;
    }
}
