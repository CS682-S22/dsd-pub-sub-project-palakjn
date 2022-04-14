package models;

import com.google.gson.annotations.Expose;

/**
 * Responsible for the holding heartbeat request values
 *
 * @author Palak Jain
 */
public class HeartBeatRequest extends Object {
    @Expose
    private String key;
    @Expose
    private String serverId;
    private String receivedId;

    public HeartBeatRequest(String key, String serverId, String receivedId) {
        this.key = key;
        this.serverId = serverId;
        this.receivedId = receivedId;
    }

    /**
     * Get the key (< topicName>:< PartitionNumber>)
     */
    public String getKey() {
        return key;
    }

    /**
     * Get the sender broker id (< address>:< port>)
     */
    public String getServerId() {
        return serverId;
    }

    /**
     * Get the receiver broker id (< address>:< port>)
     */
    public String getReceivedId() {
        return receivedId;
    }
}
