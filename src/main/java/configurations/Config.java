package configurations;

import models.Host;
import models.Topic;

import java.util.List;

public class Config {
    private Host loadBalancer;
    private boolean createTopic;
    private List<Topic> topicsToCreate;
    private boolean isProducer;
    private boolean isConsumer;
    private List<TopicConfig> topics;
    private int method;
    private String hostName;

    public Host getLoadBalancer() {
        return loadBalancer;
    }

    public boolean isCreateTopic() {
        return createTopic;
    }

    public List<Topic> getTopicsToCreate() {
        return topicsToCreate;
    }

    public List<TopicConfig> getTopics() {
        return topics;
    }

    public boolean isProducer() {
        return isProducer;
    }

    public boolean isConsumer() {
        return isConsumer;
    }

    public int getMethod() {
        return method;
    }

    public String getHostName() {
        return hostName;
    }
}
