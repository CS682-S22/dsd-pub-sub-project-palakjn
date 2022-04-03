package configurations;

import models.Host;
import models.Topic;
import models.requests.CreateTopicRequest;

import java.util.List;

/**
 * Responsible for holding broker config values
 * @author Palak Jain
 */
public class Config {
    private Host loadBalancer;
    private boolean createTopic;
    private List<CreateTopicRequest> topicsToCreate;
    private boolean isProducer;
    private boolean isConsumer;
    private List<TopicConfig> topics;
    private int method;
    private String hostName;

    /**
     * Get the load balancer host info
     */
    public Host getLoadBalancer() {
        return loadBalancer;
    }

    /**
     * Get whether to create topic or not
     */
    public boolean isCreateTopic() {
        return createTopic;
    }

    /**
     * Get the topics list to create
     */
    public List<CreateTopicRequest> getTopicsToCreate() {
        return topicsToCreate;
    }

    /**
     * Get the topic lists to produce/consume
     */
    public List<TopicConfig> getTopics() {
        return topics;
    }

    /**
     * Gets whether the host is producer
     */
    public boolean isProducer() {
        return isProducer;
    }

    /**
     * Gets whether the host is consumer
     */
    public boolean isConsumer() {
        return isConsumer;
    }

    /**
     * Get the method
     */
    public int getMethod() {
        return method;
    }

    /**
     * Get the host name
     */
    public String getHostName() {
        return hostName;
    }
}
