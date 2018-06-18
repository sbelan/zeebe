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
package io.zeebe.client.api.commands;

import io.zeebe.client.api.events.MessageEvent;
import java.io.InputStream;
import java.util.Map;

public interface PublishMessageCommandStep1 {

  PublishMessageCommandStep2 messageName(String messageName);

  interface PublishMessageCommandStep2 {

    PublishMessageCommandStep3 messageKey(String key);
  }

  interface PublishMessageCommandStep3 extends FinalCommandStep<MessageEvent> {

    /**
     * Set the initial payload of the workflow instance.
     *
     * @param payload the payload (JSON) as stream
     * @return the builder for this command. Call {@link #send()} to complete the command and send
     *     it to the broker.
     */
    PublishMessageCommandStep3 payload(InputStream payload);

    /**
     * Set the initial payload of the workflow instance.
     *
     * @param payload the payload (JSON) as String
     * @return the builder for this command. Call {@link #send()} to complete the command and send
     *     it to the broker.
     */
    PublishMessageCommandStep3 payload(String payload);

    /**
     * Set the initial payload of the workflow instance.
     *
     * @param payload the payload as map
     * @return the builder for this command. Call {@link #send()} to complete the command and send
     *     it to the broker.
     */
    PublishMessageCommandStep3 payload(Map<String, Object> payload);
  }
}
