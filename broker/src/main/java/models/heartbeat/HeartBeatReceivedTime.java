package models.heartbeat;

import configurations.BrokerConstants;

/**
 * Responsible for holding timespan of the heartbeat messages received for the broker holding particular partition.
 *
 * @author Palak Jain
 */
public class HeartBeatReceivedTime {
    private String key;
    private String serverId;
    private long timespan;
    private int retries;

    public HeartBeatReceivedTime(String key, String serverId) {
        this.key = key;
        this.serverId = serverId;
    }

    /**
     * Get the partition key
     */
    public String getKey() {
        return key;
    }

    /**
     * Get the server id
     */
    public String getServerId() {
        return serverId;
    }

    /**
     * Get the timespan
     */
    public long getTimespan() {
        return timespan;
    }

    /**
     * Set the new timespan
     */
    public void setTimespan(long timespan) {
        this.timespan = timespan;
    }

    /**
     * Increment the retry count by 1
     */
    public void incrementRetry() {
        retries++;
    }

    /**
     * Reset the retry count to 0 (As, heartbeat might have received)
     */
    public void resetRetryCount() {
        retries = 0;
    }

    /**
     * Checks whether we have retried MAX and haven't received heartbeat message
     */
    public boolean isMaxRetry() {
        return retries == BrokerConstants.HEARTBEAT_MAX_RETRY - 1;
    }
}
