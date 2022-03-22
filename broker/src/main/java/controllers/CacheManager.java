package controllers;

import models.File;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CacheManager {
    private static Map<String, File> partitions = new HashMap<>();
    private static List<String> topics = new ArrayList<>();
    private static List<Subscriber> subscribers = new ArrayList<>();
    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private CacheManager() {
    }

    public static void addTopic(String topic) {
        topics.add(topic);
    }

    public static boolean iSTopicExist(String topic) {
        return topics.contains(topic);
    }

    public static void addPartition(String topic, int partition, File file) {
        partitions.put(getKey(topic, partition), file);
    }

    public static boolean isExist(String topic, int partition) {
        return partitions.containsKey(getKey(topic, partition));
    }

    public static File getPartition(String topic, int partition) {
        return partitions.getOrDefault(getKey(topic, partition), null);
    }

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
