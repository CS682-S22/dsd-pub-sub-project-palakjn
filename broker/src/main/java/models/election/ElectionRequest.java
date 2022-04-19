package models.election;

import com.google.gson.annotations.Expose;
import models.Host;

/**
 * Responsible for sending the election request for the partition key
 *
 * @author Palak Jain
 */
public class ElectionRequest {
    @Expose
    private String key;
    @Expose
    private Host failedBroker;

    public ElectionRequest(String key, Host failedBroker) {
        this.key = key;
        this.failedBroker = failedBroker;
    }

    /**
     * Get the partition key of the topic for which the broker has to start election for
     */
    public String getKey() {
        return key;
    }

    /**
     * Get
     */
    public Host getFailedBroker() {
        return failedBroker;
    }
}
