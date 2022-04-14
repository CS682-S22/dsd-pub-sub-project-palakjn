package controllers;

import controllers.replication.Followers;
import models.File;
import models.Host;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Responsible for holding topics, subscribers information which the broker handling.
 *
 * @author Palak Jain
 */
public class CacheManager {
    private static Map<String, File> partitions = new HashMap<>();
    private static List<String> topics = new ArrayList<>();
    private static List<Subscriber> subscribers = new ArrayList<>();
    private static Map<String, Followers> followers = new HashMap<>();
    private static ReentrantReadWriteLock subscriberLock = new ReentrantReadWriteLock();
    private static ReentrantReadWriteLock followerLock = new ReentrantReadWriteLock();

    private CacheManager() {
    }

    /**
     * Add the topic to the collection
     */
    public static void addTopic(String topic) {
        topics.add(topic);
    }

    /**
     * Checks if topic with the given name exists
     */
    public static boolean iSTopicExist(String topic) {
        return topics.contains(topic);
    }

    /**
     * Add topic, partition information which broker going to handle to the map.
     */
    public static void addPartition(String topic, int partition, File file) {
        partitions.put(getKey(topic, partition), file);
    }

    /**
     * Checks if the broker handling the given partition of the topic
     */
    public static boolean isExist(String topic, int partition) {
        return partitions.containsKey(getKey(topic, partition));
    }

    /**
     * Get the file holding given partition of the topic information
     */
    public static File getPartition(String topic, int partition) {
        return partitions.getOrDefault(getKey(topic, partition), null);
    }

    /**
     * Format the key for the partitions map
     */
    public static String getKey(String topic, int partition) {
        return String.format("%s:%d", topic, partition);
    }

    /**
     * Adding subscriber to the list.
     * @param subscriber The one who wants to subscribe.
     */
    public static void addSubscriber(Subscriber subscriber) {
        subscriberLock.writeLock().lock();

        try {
            subscribers.add(subscriber);
        } finally {
            subscriberLock.writeLock().unlock();
        }
    }

    /**
     * Gets the total number of Subscriber.
     * @return size of an array
     */
    public static int getSubscribersCount() {
        subscriberLock.readLock().lock();

        try {
            return subscribers.size();
        } finally {
            subscriberLock.readLock().unlock();
        }
    }

    /**
     * Gets the subscriber object at a given index
     * @param index location of an item in an array
     * @return SubscribeHandler object if a given index is less than the size of an array else null
     */
    public static Subscriber getSubscriber(int index) {
        Subscriber subscriber;
        subscriberLock.readLock().lock();

        subscriber = subscribers.get(index);

        subscriberLock.readLock().unlock();
        return subscriber;
    }

    /**
     * Add new follower
     */
    public static void addFollower(String key, Host follower) {
        followerLock.writeLock().lock();

        Followers followerColl = followers.getOrDefault(key, new Followers());
        followerColl.add(follower);
        followers.put(key, followerColl);

        followerLock.writeLock().unlock();
    }

    /**
     * Remove the follower
     */
    public static void removeFollower(String key, Host follower) {
        followerLock.writeLock().lock();

        if (followers.containsKey(key)) {
            Followers followerColl = followers.get(key);
            followerColl.remove(follower);
        }

        followerLock.writeLock().unlock();
    }

    /**
     * Get all the followers handling the particular key
     */
    public static Followers getFollowers(String key) {
        followerLock.readLock().lock();

        try {
            return followers.getOrDefault(key, null);
        } finally {
            followerLock.readLock().unlock();
        }
    }
}
