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
import io.zeebe.msgpack.property.BinaryProperty;
import io.zeebe.msgpack.property.StringProperty;
import io.zeebe.msgpack.spec.MsgPackHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class MessageRecord extends UnpackedObject {
  public static final DirectBuffer NO_PAYLOAD = new UnsafeBuffer(MsgPackHelper.NIL);

  public static final String PROP_MESSAGE_NAME = "messageName";
  public static final String PROP_MESSAGE_KEY = "messageKey";
  public static final String PROP_MESSAGE_PAYLOAD = "payload";

  private final StringProperty messageNameProp = new StringProperty(PROP_MESSAGE_NAME, "");
  private final StringProperty messageKeyProp = new StringProperty(PROP_MESSAGE_KEY, "");
  private final BinaryProperty payloadProp = new BinaryProperty(PROP_MESSAGE_PAYLOAD, NO_PAYLOAD);

  public MessageRecord() {
    this.declareProperty(messageNameProp)
        .declareProperty(messageKeyProp)
        .declareProperty(payloadProp);
  }

  public MessageRecord setMessageName(DirectBuffer directBuffer) {
    messageNameProp.setValue(directBuffer);
    return this;
  }

  public MessageRecord setMessageKey(DirectBuffer directBuffer) {
    messageKeyProp.setValue(directBuffer);
    return this;
  }

  public DirectBuffer getMessageName() {
    return messageNameProp.getValue();
  }

  public DirectBuffer getMessageKey() {
    return messageKeyProp.getValue();
  }

  public DirectBuffer getPayload() {
    return payloadProp.getValue();
  }

  public MessageRecord setPayload(DirectBuffer payload) {
    payloadProp.setValue(payload);
    return this;
  }

  public MessageRecord setPayload(DirectBuffer payload, int offset, int length) {
    payloadProp.setValue(payload, offset, length);
    return this;
  }
}
