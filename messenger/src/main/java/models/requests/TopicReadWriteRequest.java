package models.requests;

import com.google.gson.annotations.Expose;
import utilities.Strings;

/**
 * Responsible for holding values for reading/writing data from/into partition of topic.
 *
 * @author Palak Jain
 */
public class TopicReadWriteRequest {
    @Expose
    private String name;
    @Expose
    private int partition;
    @Expose
    private int fromOffset;
    @Expose
    private int numOfMsg;
    @Expose
    private int toOffset;
    @Expose
    private String receiver;

    public TopicReadWriteRequest(String name, int partition, int offset, int numOfMsg) {
        this.name = name;
        this.partition = partition;
        this.fromOffset = offset;
        this.numOfMsg = numOfMsg;
    }

    public TopicReadWriteRequest(String name, int partition, int offset) {
        this.name = name;
        this.partition = partition;
        this.fromOffset = offset;
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
     * Get the offset to read from
     */
    public int getFromOffset() {
        return fromOffset;
    }

    /**
     * Set the offset to read from
     */
    public void setFromOffset(int offset) {
        this.fromOffset = offset;
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

    /**
     * Get the offset to read till
     */
    public int getToOffset() {
        return toOffset;
    }

    /**
     * Set the offset to read till
     */
    public void setToOffset(int lastOffset) {
        this.toOffset = lastOffset;
    }

    /**
     * Get the receiver broker of the topic
     */
    public String getReceiver() {
        return receiver;
    }
    /**
     * Set the broker who is going to receive the topic
     */
    public void setReceiver(String receiver) {
        this.receiver = receiver;
    }
}
