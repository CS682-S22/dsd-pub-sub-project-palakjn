package models;

import configuration.Constants;
import utilities.Strings;

/**
 * Responsible for holding information of a request to the host
 *
 * @author Palak Jain
 */
public class Request extends Object {
    private int type; //Topic or Partition
    private String topicName;
    private int partition;
    private int offset;
    private int numOfMsg;

    public Request(int type, String topicName, int partition) {
        this.type = type;
        this.topicName = topicName;
        this.partition = partition;
    }

    public Request(int type, String topicName, int partition, int offset) {
        this.type = type;
        this.topicName = topicName;
        this.partition = partition;
        this.offset = offset;
    }
    public Request(int type, String topicName, int partition, int offset, int numOfMsg) {
        this.type = type;
        this.topicName = topicName;
        this.partition = partition;
        this.offset = offset;
        this.numOfMsg = numOfMsg;
    }

    public Request() {}

    /**
     * Get the type of the request (Topic/Partition)
     */
    public int getType() {
        return type;
    }

    /**
     * Get the name of the topic
     */
    public String getTopicName() {
        return topicName;
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
        return Constants.findRequestByValue(type) != null && !Strings.isNullOrEmpty(topicName);
    }
}
