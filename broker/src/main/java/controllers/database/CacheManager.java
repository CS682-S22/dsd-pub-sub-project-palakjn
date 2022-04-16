package controllers.database;

import configurations.BrokerConstants;
import controllers.consumer.Subscriber;
import controllers.replication.Broker;
import controllers.replication.Brokers;
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
    //Priority number of the broker
    private volatile static int priorityNum;

    //Status of the broker for the partition it is holding
    private static Map<String, BrokerConstants.BROKER_STATE> brokerStatus = new HashMap<>();

    //Partitions information
    private static Map<String, File> partitions = new HashMap<>();
    private static List<String> topics = new ArrayList<>();

    //List of consumers subscribed for the logs
    private static List<Subscriber> subscribers = new ArrayList<>();

    //Leaders and brokers information which are handling particular topic and partition (Membership table)
    private static Map<String, Broker> leaders = new HashMap<>();
    private static Map<String, Brokers> brokers = new HashMap<>();

    //Lock to make data structures thread-safe
    private static ReentrantReadWriteLock subscriberLock = new ReentrantReadWriteLock();
    private static ReentrantReadWriteLock leadersLock = new ReentrantReadWriteLock();
    private static ReentrantReadWriteLock brokerLock = new ReentrantReadWriteLock();

    private CacheManager() {
    }

    /**
     * Get the priority number assigned to the broker
     */
    public static int getPriorityNum() {
        return priorityNum;
    }

    /**
     * Set the priority number
     */
    public static void setPriorityNum(int priorityNum) {
        CacheManager.priorityNum = priorityNum;
    }

    /**
     * Get the state mode of the broker which is holding the particular key
     */
    public BrokerConstants.BROKER_STATE getStatus(String key) {
        return brokerStatus.getOrDefault(key, BrokerConstants.BROKER_STATE.NONE);
    }

    /**
     * Set the state mode of the broker
     */
    public void setStatus(String key, BrokerConstants.BROKER_STATE state) {
        brokerStatus.put(key, state);
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
     * Set the leader for the given partition key
     */
    public static void setLeader(String key, Broker leader) {
        leadersLock.writeLock().lock();

        if (leader != null) {
            leaders.put(key, leader);
        }

        leadersLock.writeLock().unlock();
    }

    /**
     * Set the leader as null for the given partition key
     */
    public static void setLeaderAsInActive(String key) {
        leadersLock.writeLock().lock();

        Broker leader = leaders.getOrDefault(key, null);
        if (leader != null) {
            leader.setInActive();
        }

        leadersLock.writeLock().unlock();
    }

    /**
     * Get the leader which is holding the given key
     */
    public static Broker getLeader(String key) {
        leadersLock.readLock().lock();

        try {
            return leaders.getOrDefault(key, null);
        } finally {
            leadersLock.readLock().unlock();
        }
    }

    /**
     * Checks if the given host is the leader of the given partition key
     */
    public static boolean isLeader(String key, Host host) {
        leadersLock.readLock().lock();
        boolean isEqual = false;

        Broker leader = leaders.getOrDefault(key, null);

        if (leader != null) {
            isEqual = leader.equals(host);
        }

        leadersLock.readLock().unlock();
        return isEqual;
    }

    /**
     * Check if the leader holding the partition key is failed.
     */
    public static boolean isLeaderFail(String key) {
        boolean isFailed = true;
        leadersLock.readLock().lock();

        Broker leader = leaders.getOrDefault(key, null);
        if (leader != null) {
            isFailed = !leader.isActive();
        }

        leadersLock.readLock().unlock();
        return isFailed;
    }

    /**
     * Add new broker
     */
    public static void addBroker(String key, Broker broker) {
        brokerLock.writeLock().lock();

        Brokers brokerColl = brokers.getOrDefault(key, new Brokers());
        brokerColl.add(broker);
        brokers.put(key, brokerColl);

        brokerLock.writeLock().unlock();
    }

    /**
     * Remove the broker
     */
    public static void removeBroker(String key, Broker broker) {
        brokerLock.writeLock().lock();

        if (brokers.containsKey(key)) {
            Brokers brokerColl = brokers.get(key);
            brokerColl.remove(broker);
        }

        brokerLock.writeLock().unlock();
    }

    /**
     * Get all the brokers handling the particular key
     */
    public static Brokers getBrokers(String key) {
        brokerLock.readLock().lock();

        try {
            return brokers.getOrDefault(key, null);
        } finally {
            brokerLock.readLock().unlock();
        }
    }
}
