package models.requests;

import utilities.Strings;

public class GetBrokerRequest {
    private String name;
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
