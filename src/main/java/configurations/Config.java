package configurations;

import models.Host;
import models.Topic;

import java.util.List;

public class Config {
    private Host loadBalancer;
    private boolean createTopic;
    private List<Topic> topics;
    private boolean isProducer;
    private boolean isConsumer;
    private String location;

    public Host getLoadBalancer() {
        return loadBalancer;
    }

    public boolean isCreateTopic() {
        return createTopic;
    }

    public List<Topic> getTopics() {
        return topics;
    }

    public boolean isProducer() {
        return isProducer;
    }

    public boolean isConsumer() {
        return isConsumer;
    }

    public String getLocation() {
        return location;
    }
}
