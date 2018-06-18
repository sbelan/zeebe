package io.zeebe.client.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import io.zeebe.client.api.commands.Topic;
import io.zeebe.client.api.commands.Topics;
import io.zeebe.client.impl.topic.TopicsRequestImpl;
import org.slf4j.Logger;

public class ClientTopicsMetada {

    private final static Logger LOG = Loggers.CLIENT_LOGGER;

    private volatile Map<String, Topic> topics = new HashMap<>();

    private final RequestManager requestManager;

    public ClientTopicsMetada(RequestManager requestManager) {
        this.requestManager = requestManager;
    }

    public Topic getTopic(String name, long deadline) {

        long refreshBackoff = 1;

        do
        {
            final Topic topic = topics.get(name);

            if (topic != null)
            {
                return topic;
            }
            else
            {
                try
                {
                    Thread.sleep(refreshBackoff);
                }
                catch (InterruptedException e) {
                    return null;
                }

                refresh();

                refreshBackoff += Math.min(refreshBackoff * 2, 1000);
            }
        }
        while(System.currentTimeMillis() < deadline);

        return null;
    }

    private void refresh()
    {
        try
        {
            Topics topics = new TopicsRequestImpl(requestManager).send().join(5, TimeUnit.SECONDS);

            final Map<String, Topic> mappedTopics = new HashMap<>();

            for (Topic t : topics.getTopics()) {
                mappedTopics.put(t.getName(), t);
            }

            this.topics = mappedTopics;
        }
        catch (RuntimeException e) {
            LOG.debug("Exception while fetching topic metadata", e);
        }
    }
}
