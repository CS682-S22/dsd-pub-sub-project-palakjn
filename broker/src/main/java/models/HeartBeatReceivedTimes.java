package models;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Responsible for maintaining the collection of timespan of heartbeat messages received from all the brokers
 *
 * @author Palak Jain
 */
public class HeartBeatReceivedTimes {
    private List<HeartBeatReceivedTime> heartBeatReceivedTimes;
    private ReentrantReadWriteLock lock;

    public HeartBeatReceivedTimes() {
        heartBeatReceivedTimes = new ArrayList<>();
        lock = new ReentrantReadWriteLock();
    }

    /**
     * Add the new heartbeat timespan of the given server holding given partition key
     */
    public void add(String key, String serverId) {
        lock.writeLock().lock();

        heartBeatReceivedTimes.add(new HeartBeatReceivedTime(key, serverId));

        lock.writeLock().unlock();
    }

    /**
     * Remove the heartbeat timespan of the given server holding given partition key
     */
    public void remove(String key, String serverId) {
        lock.writeLock().lock();

        heartBeatReceivedTimes.removeIf(entry -> entry.getKey().equals(key) && entry.getServerId().equals(serverId));

        lock.writeLock().unlock();
    }

    /**
     * Get the heartbeat timespan of the given server holding given partition key
     */
    public HeartBeatReceivedTime get(String key, String serverId) {
        HeartBeatReceivedTime heartBeatReceivedTime = null;
        lock.readLock().lock();

        if (exist(key, serverId)) {
            heartBeatReceivedTime = (HeartBeatReceivedTime) heartBeatReceivedTimes.stream().filter(entry -> entry.getKey().equals(key) && entry.getServerId().equals(serverId));
        }

        lock.readLock().unlock();
        return  heartBeatReceivedTime;
    }

    /**
     * Checks whether holding the timespan of the given server holding given partition key
     */
    public boolean exist(String key, String serverId) {
        lock.readLock().lock();

        try {
            return heartBeatReceivedTimes.stream().anyMatch(entry -> entry.getKey().equals(key) && entry.getServerId().equals(serverId));
        } finally {
            lock.readLock().unlock();
        }
    }
}
