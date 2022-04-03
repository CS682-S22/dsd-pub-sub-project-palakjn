package models.requests;

import utilities.Strings;

public class TopicReadWriteRequest {
    private String name;
    private int partition;
    private int offset;
    private int numOfMsg;

    public TopicReadWriteRequest(String name, int partition, int offset, int numOfMsg) {
        this.name = name;
        this.partition = partition;
        this.offset = offset;
        this.numOfMsg = numOfMsg;
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
     * Get the offset
     */
    public int getOffset() {
        return offset;
    }

    /**
     * Set the offset
     */
    public void setOffset(int offset) {
        this.offset = offset;
    }

    /**
     * Get number of logs to request
     */
    public int getNumOfMsg() {
        return numOfMsg;
    }

    /**
     * Validates whether the request contains required values or not
     */
    public boolean isValid() {
        return !Strings.isNullOrEmpty(name);
    }
}
