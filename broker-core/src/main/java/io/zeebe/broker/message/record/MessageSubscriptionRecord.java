/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.message.record;

import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.msgpack.property.*;
import org.agrona.DirectBuffer;

public class MessageSubscriptionRecord extends UnpackedObject {

  public static final String PROP_MESSAGE_NAME = "messageName";
  public static final String PROP_MESSAGE_KEY = "messageKey";

  public static final String PROP_PARTITION_ID = "partitionId";
  public static final String PROP_WORKFLOW_INSTANCE_KEY = "workflowInstanceKey";
  public static final String PROP_ACTIVITY_INSTANCE_ID = "activityInstanceId";

  private final StringProperty messageNameProp = new StringProperty(PROP_MESSAGE_NAME, "");
  private final StringProperty messageKeyProp = new StringProperty(PROP_MESSAGE_KEY, "");
  private final IntegerProperty partitionIdProp = new IntegerProperty(PROP_PARTITION_ID);
  private final LongProperty workflowInstanceKeyProp = new LongProperty(PROP_WORKFLOW_INSTANCE_KEY);
  private final LongProperty activityInstanceIdProp = new LongProperty(PROP_ACTIVITY_INSTANCE_ID);

  public MessageSubscriptionRecord() {
    this.declareProperty(messageNameProp)
        .declareProperty(messageKeyProp)
        .declareProperty(partitionIdProp)
        .declareProperty(workflowInstanceKeyProp)
        .declareProperty(activityInstanceIdProp);
  }

  public MessageSubscriptionRecord setMessageName(DirectBuffer directBuffer) {
    messageNameProp.setValue(directBuffer);
    return this;
  }

  public MessageSubscriptionRecord setMessageKey(DirectBuffer directBuffer) {
    messageKeyProp.setValue(directBuffer);
    return this;
  }

  public DirectBuffer getMessageName() {
    return messageNameProp.getValue();
  }

  public DirectBuffer getMessageKey() {
    return messageKeyProp.getValue();
  }

  public int getParitionId() {
    return partitionIdProp.getValue();
  }

  public long getWorkflowInstanceKey() {
    return workflowInstanceKeyProp.getValue();
  }

  public long getActivityInstanceId() {
    return activityInstanceIdProp.getValue();
  }

  public MessageSubscriptionRecord setPartitionId(int val) {
    partitionIdProp.setValue(val);
    return this;
  }

  public MessageSubscriptionRecord setWorkflowInstanceKey(long val) {
    workflowInstanceKeyProp.setValue(val);
    return this;
  }

  public MessageSubscriptionRecord setActivityInstaneId(long val) {
    activityInstanceIdProp.setValue(val);
    return this;
  }
}
