package models.sync;

import com.google.gson.annotations.Expose;
import controllers.Broker;

/**
 * Responsible for holding the response for the request of the offset from the broker
 *
 * @author Palak Jain
 */
public class OffsetResponse {
    @Expose
    private String key;
    @Expose
    private int offset;
    @Expose
    private int size;
    private Broker brokerInfo;

    public OffsetResponse(String key, int offset, int size) {
        this.key = key;
        this.offset = offset;
        this.size = size;
    }

    /**
     * Get the partition key of the topic
     */
    public String getKey() {
        return key;
    }

    /**
     * Get the offset which the broker has for the partition - topic
     */
    public int getOffset() {
        return offset;
    }

    /**
     * Get the broker info which stores the partition
     */
    public Broker getBrokerInfo() {
        return brokerInfo;
    }

    /**
     * Set the broker info which stores the partition
     */
    public void setBrokerInfo(Broker brokerInfo) {
        this.brokerInfo = brokerInfo;
    }

    /**
     * Get the total size
     */
    public int getSize() {
        return size;
    }
}
