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
    private int offset;
    @Expose
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
