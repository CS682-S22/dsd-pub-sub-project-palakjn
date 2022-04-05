package models.requests;

import com.google.gson.annotations.Expose;
import models.Host;

/**
 * Responsible for holding failed broker information.
 *
 * @author Palak Jain
 */
public class FailBrokerRequest {
    @Expose
    private String topic;
    @Expose
    private int partition;
    @Expose
    private Host broker;

    public FailBrokerRequest(String topic, int partition, Host broker) {
        this.topic = topic;
        this.partition = partition;
        this.broker = broker;
    }

    /**
     * Get the topic name which failed broker was handling
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Get the partition number of the topic which failed broker was handling
     */
    public int getPartition() {
        return partition;
    }

    /**
     * Get the failed broker information
     */
    public Host getBroker() {
        return broker;
    }
}
