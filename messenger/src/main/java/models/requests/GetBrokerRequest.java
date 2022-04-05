package models.requests;

import com.google.gson.annotations.Expose;
import utilities.Strings;

/**
 * Responsible for holding values for getting broker details holding partition information of topic.
 *
 * @author Palak Jain
 */
public class GetBrokerRequest {
    @Expose
    private String name;
    @Expose
    private int partition;

    public GetBrokerRequest(String name, int partition) {
        this.name = name;
        this.partition = partition;
    }

    /**
     * Get the name of the topic
     */
    public String getName() {
        return name;
    }

    /**
     * Get the partition number of the topic
     */
    public int getPartition() {
        return partition;
    }

    /**
     * Validates whether the request contains required values or not
     */
    public boolean isValid() {
        return !Strings.isNullOrEmpty(name);
    }
}
