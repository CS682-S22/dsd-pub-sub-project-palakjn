package models.requests;

import com.google.gson.annotations.Expose;
import models.Host;
import utilities.Strings;

/**
 * Responsible for holding failed broker information.
 *
 * @author Palak Jain
 */
public class BrokerUpdateRequest {
    @Expose
    private String topic;
    @Expose
    private int partition;
    @Expose
    private String key;
    @Expose
    private Host broker;

    public BrokerUpdateRequest(String key, Host broker) {
        String[] parts = key.split(":");
        this.topic = parts[0];
        this.key = key;
        this.partition = Integer.parseInt(parts[1]);
        this.broker = broker;
    }

    /**
     * Get the partition key
     */
    public String getKey() {
        return key;
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

    /**
     * Checks whether the request is valid or not
     */
    public boolean isValid() {
        return !Strings.isNullOrEmpty(topic) && broker != null && broker.isValid();
    }
}
