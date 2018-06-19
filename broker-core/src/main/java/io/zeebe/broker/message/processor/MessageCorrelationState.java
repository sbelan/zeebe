package io.zeebe.broker.message.processor;

import java.util.*;

public class MessageCorrelationState {

    private Map<String, List<MessageSubscriptionInfo>> activeSubscriptions = new HashMap<>();

    private Map<String, List<MessageInfo>> uncorrelatedMessages = new HashMap<>();

    public MessageInfo getNextMessage(String messageName, String messageKey)
    {
        final String messageId = messageName + messageKey;

        final List<MessageInfo> messagesForId = uncorrelatedMessages.get(messageId);

        if (messagesForId != null && !messagesForId.isEmpty())
        {
            return messagesForId.remove(0);
        }
        else
        {
            return null;
        }
    }

    public MessageSubscriptionInfo getNextSubscription(String messageName, String messageKey)
    {
        final String messageId = messageName + messageKey;

        final List<MessageSubscriptionInfo> subscriptionsForId = activeSubscriptions.get(messageId);

        if (subscriptionsForId != null && !subscriptionsForId.isEmpty())
        {
            return subscriptionsForId.remove(0);
        }
        else
        {
            return null;
        }
    }

    public void addMessage(MessageInfo msg)
    {
        final String messageId = msg.messageName + msg.messageKey;

        List<MessageInfo> messagesForId = uncorrelatedMessages.get(messageId);

        if (messagesForId == null)
        {
            messagesForId = new ArrayList<>();
            uncorrelatedMessages.put(messageId, messagesForId);
        }

        messagesForId.add(msg);
    }

    public void addSubscription(MessageSubscriptionInfo subscription)
    {
        final String messageId = subscription.messageName + subscription.messageKey;

        List<MessageSubscriptionInfo> subscriptionsForId = activeSubscriptions.get(messageId);

        if (subscriptionsForId == null)
        {
            subscriptionsForId = new ArrayList<>();
            activeSubscriptions.put(messageId, subscriptionsForId);
        }

        subscriptionsForId.add(subscription);
    }

    public static class MessageInfo {
        String messageName;
        String messageKey;
        byte[] payload;
    }

    public static class MessageSubscriptionInfo {
        String messageName;
        String messageKey;
        int partitionId;
        long workflowInstanceKey;
        long activityInstanceId;
    }
}
