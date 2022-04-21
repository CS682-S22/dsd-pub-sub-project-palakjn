package models.heartbeat;

import java.util.ArrayList;
import java.util.List;

/**
 * Responsible for maintaining the collection of timespan of heartbeat messages received from all the brokers
 *
 * @author Palak Jain
 */
public class HeartBeatReceivedTimes {
    private List<HeartBeatReceivedTime> heartBeatReceivedTimes;

    public HeartBeatReceivedTimes() {
        heartBeatReceivedTimes = new ArrayList<>();
    }

    /**
     * Add the new heartbeat timespan of the given server holding given partition key
     */
    public void add(String key, String serverId) {
        heartBeatReceivedTimes.add(new HeartBeatReceivedTime(key, serverId));
    }

    /**
     * Remove the heartbeat timespan of the given server holding given partition key
     */
    public void remove(String key, String serverId) {
        heartBeatReceivedTimes.removeIf(entry -> entry.getKey().equals(key) && entry.getServerId().equals(serverId));
    }

    /**
     * Get the heartbeat timespan of the given server holding given partition key
     */
    public HeartBeatReceivedTime get(String key, String serverId) {
        HeartBeatReceivedTime heartBeatReceivedTime = null;

        if (exist(key, serverId)) {
            for (HeartBeatReceivedTime receivedTime : heartBeatReceivedTimes) {
                if (receivedTime.getServerId().equals(serverId) && receivedTime.getKey().equals(key)) {
                    heartBeatReceivedTime = receivedTime;
                    break;
                }
            }
        }

        return  heartBeatReceivedTime;
    }

    /**
     * Checks whether holding the timespan of the given server holding given partition key
     */
    public boolean exist(String key, String serverId) {
        return heartBeatReceivedTimes.stream().anyMatch(entry -> entry.getKey().equals(key) && entry.getServerId().equals(serverId));
    }
}
