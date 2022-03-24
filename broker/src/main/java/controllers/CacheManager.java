package controllers;

import models.File;

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
    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

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
    private static String getKey(String topic, int partition) {
        return String.format("%s_%d", topic, partition);
    }

    /**
     * Adding subscriber to the list.
     * @param subscriber The one who wants to subscribe.
     */
    public static void addSubscriber(Subscriber subscriber) {
        lock.writeLock().lock();

        try {
            subscribers.add(subscriber);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Gets the total number of Subscriber.
     * @return size of an array
     */
    public static int getSubscribersCount() {
        lock.readLock().lock();

        try {
            return subscribers.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets the subscriber object at a given index
     * @param index location of an item in an array
     * @return SubscribeHandler object if a given index is less than the size of an array else null
     */
    public static Subscriber getSubscriber(int index) {
        Subscriber subscriber;
        lock.readLock().lock();

        subscriber = subscribers.get(index);

        lock.readLock().unlock();
        return subscriber;
    }
}
