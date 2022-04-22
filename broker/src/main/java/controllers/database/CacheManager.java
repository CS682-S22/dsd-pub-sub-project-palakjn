package controllers.database;

import configurations.BrokerConstants;
import controllers.Broker;
import controllers.Brokers;
import controllers.consumer.Subscriber;
import models.Host;
import models.data.File;
import models.heartbeat.HeartBeatReceivedTime;
import models.heartbeat.HeartBeatReceivedTimes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private static final Logger logger = LogManager.getLogger(CacheManager.class);

    //Details of local broker
    private static Host broker;

    //Details of load balancer
    private static Host loadBalancer;

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

    //Heartbeat
    private static HeartBeatReceivedTimes heartBeatReceivedTimes = new HeartBeatReceivedTimes();

    //Lock to make data structures thread-safe
    private static ReentrantReadWriteLock topicLock = new ReentrantReadWriteLock();
    private static ReentrantReadWriteLock subscriberLock = new ReentrantReadWriteLock();
    private static ReentrantReadWriteLock leadersLock = new ReentrantReadWriteLock();
    private static ReentrantReadWriteLock brokerLock = new ReentrantReadWriteLock();
    private static ReentrantReadWriteLock heartBeatLock = new ReentrantReadWriteLock();

    private CacheManager() {
    }

    /**
     * Get the current running broker info
     */
    public static Host getBrokerInfo() {
        return broker;
    }

    /**
     * Set the current running broker details
     */
    public static void setBroker(Host host) {
        broker = host;
    }

    /**
     * Get the load balancer details
     */
    public static Host getLoadBalancer() {
        return loadBalancer;
    }

    /**
     * Set the load balancer details
     */
    public static void setLoadBalancer(Host loadBalancer) {
        CacheManager.loadBalancer = loadBalancer;
    }

    /**
     * Set the priority number of the current running broker
     */
    public static void setPriorityNum(int priorityNum) {
        broker.setPriorityNum(priorityNum);
    }

    /**
     * Get the priority number of the current running broker
     */
    public static int getPriorityNum() {
        return broker.getPriorityNum();
    }

    /**
     * Get the state mode of the broker which is holding the particular key
     */
    public static synchronized BrokerConstants.BROKER_STATE getStatus(String key) {
        return brokerStatus.getOrDefault(key, BrokerConstants.BROKER_STATE.NONE);
    }

    /**
     * Set the state mode of the broker
     */
    public static synchronized void setStatus(String key, BrokerConstants.BROKER_STATE state) {
        brokerStatus.put(key, state);
    }

    /**
     * Add the topic to the collection
     */
    public static void addTopic(String topic) {
        topicLock.writeLock().lock();

        topics.add(topic);

        topicLock.writeLock().unlock();
    }

    /**
     * Checks if topic with the given name exists
     */
    public static boolean iSTopicExist(String topic) {
        topicLock.readLock().lock();

        try {
            return topics.contains(topic);
        } finally {
            topicLock.readLock().unlock();
        }
    }

    /**
     * Add topic, partition information which broker going to handle to the map.
     */
    public static void addPartition(String topic, int partition, File file) {
        topicLock.writeLock().lock();

        partitions.put(getKey(topic, partition), file);

        topicLock.writeLock().unlock();
    }

    /**
     * Update partition in a map
     */
    public static void updatePartition(String key, File file) {
        topicLock.writeLock().lock();

        partitions.put(key, file);

        File partition = getPartition(key);

        //TODO: Remove
        logger.info("Updated partition with the offset as " + partition.getOffset() + " and total size as " + partition.getTotalSize());

        topicLock.writeLock().unlock();
    }

    /**
     * Checks if the broker handling the given partition of the topic
     */
    public static boolean isExist(String topic, int partition) {
        topicLock.readLock().lock();

        try {
            return partitions.containsKey(getKey(topic, partition));
        } finally {
            topicLock.readLock().unlock();
        }
    }

    /**
     * Get the file holding given partition of the topic information
     */
    public static File getPartition(String topic, int partition) {
        topicLock.readLock().lock();

        try {
            return partitions.getOrDefault(getKey(topic, partition), null);
        } finally {
            topicLock.readLock().unlock();
        }
    }

    /**
     * Get the file holding given partition of the topic information
     */
    public static File getPartition(String key) {
        topicLock.readLock().lock();

        try {
            return partitions.getOrDefault(key, null);
        } finally {
            topicLock.readLock().unlock();
        }
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
            isEqual = leader.isSame(host);

            if (!isEqual) {
                //TODO: Remove
                logger.info("[" + broker.getString() + "] Broker " + host.getString() + " is not the leader of topic " + key + " Leader is: " + leader.getString());
            }
        } else {
            //TODO: Remove
            logger.info("[" + broker.getString() + "] No leader info found");
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

        if (!isExist(key, broker)) {
            Brokers brokerColl = brokers.getOrDefault(key, new Brokers());
            brokerColl.add(broker);
            brokers.put(key, brokerColl);
        }

        brokerLock.writeLock().unlock();
    }

    /**
     * Remove the broker
     */
    public static void removeBroker(String key, Host broker) {
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

    /**
     * Set the given broker as outdated for the given key
     */
    public static void setBrokerAsOutOfSync(String key, Broker broker, int nextOffset) {
        brokerLock.writeLock().lock();

        Brokers brokers = getBrokers(key);
        if (brokers != null) {
            brokers.setAsOutdated(broker, nextOffset);
        }

        brokerLock.writeLock().unlock();
    }

    /**
     * Update local membership table according to the received brokers from load balancer.
     */
    public static void updateMembershipTable(String key, Brokers receivedBrokers, List<Host> heartbeat) {
        brokerLock.writeLock().lock();

        //Iterating through received brokers to check if broker exist
        List<Broker> brokerList = receivedBrokers.getBrokers();
        for (Host broker : brokerList) {
            if(!isExist(key, broker)) {
                logger.info(String.format("[%s] Received new follower %s information from load balancer to handle partition %s.", CacheManager.getBrokerInfo().getString(), broker.getString(), key));
                addBroker(key, new Broker(broker));
                heartbeat.add(broker);
            }
        }

        //Iterating through have brokers to check which one is missing in received brokers
        Brokers collection = brokers.getOrDefault(key, null);
        if (collection != null) {
            brokerList = collection.getBrokers();

            for (Broker broker : brokerList) {
                if (!receivedBrokers.contains(broker)) {
                    logger.info(String.format("[%s] Missing broker %s information in the latest membership table received from load balancer to handle partition %s.", CacheManager.getBrokerInfo().getString(), broker.getString(), key));
                    collection.remove(broker);
                }
            }
        }

        brokerLock.writeLock().unlock();
    }

    /**
     * Get the membership table of the partition in string format
     */
    public static String getMemberShipTable(String key) {
        brokerLock.readLock().lock();
        StringBuilder stringBuilder = new StringBuilder();
        Host leader = getLeader(key);

        if (leader != null) {
            stringBuilder.append(String.format("Leader: %s", leader.getString()));

            Brokers collection = getBrokers(key);

            if (collection != null) {
                List<Broker> brokerList = collection.getBrokers();

                for (Host broker : brokerList) {
                    stringBuilder.append(String.format(", Broker: %s", broker.getString()));
                }
            }
        }

        brokerLock.readLock().unlock();
        return stringBuilder.toString();
    }

    /**
     * Removing broker from the partition and change the status of the broker to wait for follower
     */
    public static Broker markDownBroker(String key, String serverId) {
        Broker broker;
        brokerLock.writeLock().lock();

        broker = CacheManager.getBroker(key, serverId);
        if (broker != null) {
            if(CacheManager.isLeader(key, broker)) {
                CacheManager.setLeaderAsInActive(key);
                broker.setDesignation(BrokerConstants.BROKER_DESIGNATION.LEADER.getValue());
            } else {
                broker.setDesignation(BrokerConstants.BROKER_DESIGNATION.FOLLOWER.getValue());
            }

            removeBroker(key, broker);
            setStatus(key, BrokerConstants.BROKER_STATE.WAIT_FOR_NEW_FOLLOWER);
            logger.info(String.format("[%s] Changing the status of broker to WAIT FOR NEW FOLLOWER when %s broker failed for the partition %s.", CacheManager.getBrokerInfo().getString(), serverId, key));
        }

        brokerLock.writeLock().unlock();
        return broker;
    }

    /**
     * Get the list of brokers which has high priority number than current broker priority number
     */
    public static List<Broker> getBrokers(String key, BrokerConstants.PRIORITY_CHOICE priority_choice) {
        List<Broker> brokers = null;
        brokerLock.readLock().lock();

        Brokers collection = getBrokers(key);

        if (collection != null) {
            brokers = collection.getBrokers(getPriorityNum(), priority_choice);
        }

        brokerLock.readLock().unlock();
        return brokers;
    }

    /**
     * Get the broker with mentioned serverId handling given partition key
     */
    public static Broker getBroker(String key, String serverId) {
        Broker broker;
        brokerLock.readLock().lock();

        Brokers brokers = getBrokers(key);
        broker = brokers.getBroker(new Broker(serverId));

        brokerLock.readLock().unlock();
        return broker;
    }

    /**
     * Checks whether the given broker handling the partition of the topic
     */
    public static boolean isExist(String key, Host broker) {
        boolean exist;
        brokerLock.readLock().lock();

        Brokers brokers = getBrokers(key);
        exist = brokers != null && brokers.contains(broker);

        brokerLock.readLock().unlock();
        return exist;
    }

    /**
     * Get the heartbeat details for the given partition key and serverId
     */
    public static HeartBeatReceivedTime getHeartBeatReceivedTime(String key, String serverId) {
        heartBeatLock.readLock().lock();

        try {
            return heartBeatReceivedTimes.get(key, serverId);
        } finally {
            heartBeatLock.readLock().unlock();
        }
    }

    /**
     * Add new heartbeat details for the given partition key and serverId
     */
    public static void createHeartBeatObject(String key, String serverId) {
        heartBeatLock.writeLock().lock();

        heartBeatReceivedTimes.add(key, serverId);

        heartBeatLock.writeLock().unlock();
    }
}
