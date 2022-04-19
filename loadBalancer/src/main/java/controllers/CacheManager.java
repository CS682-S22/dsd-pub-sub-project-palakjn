package controllers;

import configuration.Constants;
import models.*;
import models.requests.BrokerUpdateRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Static global cache alike class storing the configuration of the system.
 *
 * @author Palak Jain
 */
public class CacheManager {
    private static final Logger logger = LogManager.getLogger(CacheManager.class);
    private static List<Host> brokers = new ArrayList<>();
    private static Map<String, Topic> topicMap = new HashMap<>();            // Map topic name to the topic object. Useful when a customer need to read from all the partitions of a topic
    private static Map<String, Partition> partitionMap = new HashMap<>();    // Map topic name + partition to the partition object. Useful when a customer wants to read from particular topic partition as well as when the producer wants to publish message

    private static int counter; // Counter which will incremented by one whenever new broker is joined.

    //locks
    private static final ReentrantReadWriteLock brokerLock = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock topicLock = new ReentrantReadWriteLock();

    private CacheManager() {
    }

    /**
     * Add broker if not exist to the collection
     */
    public static Host addBroker(Host broker) {
        brokerLock.writeLock().lock();

        //Getting existing broker
        Host existingBroker = getBroker(broker.getAddress(), broker.getPort());

        if (existingBroker == null) {
            //Newly joined broker

            //Giving the broker a priority number
            broker.setPriorityNum(counter);

            brokers.add(broker);

            //Incrementing the counter by 1
            counter++;
        } else {
            //Maybe the broker recover from failure and tries to join the network. Making that broker to active.
            existingBroker.setActive();
            broker = existingBroker;
        }

        brokerLock.writeLock().unlock();
        return broker;
    }

    /**
     * Remove broker from the collection
     */
    public static void removeBroker(Host broker) {
        brokerLock.writeLock().lock();
        brokers.removeIf(host -> host.getAddress().equals(broker.getAddress()) && host.getPort() == broker.getPort());
        brokerLock.writeLock().unlock();
    }

    /**
     * Find the n number of brokers which is holding less numb of partitions.
     */
    public static Brokers findBrokersWithLessLoad(int num) {
        Brokers brokersWithLessLoad = new Brokers();
        brokerLock.writeLock().lock();

        if (brokers.size() > 0) {
            for (int i = 1; i <= num; i++) {
                Host broker = null;
                int min = Integer.MAX_VALUE;

                for (Host host : brokers) {
                    if (host.isActive() && !brokersWithLessLoad.contains(host) && host.getNumberOfPartitions() < min) {
                        broker = host;
                        min = broker.getNumberOfPartitions();
                    }
                }

                if (broker != null) {
                    broker.incrementNumOfPartitions();
                    brokersWithLessLoad.add(new Host(broker));
                }
            }
        }

        brokerLock.writeLock().unlock();
        return brokersWithLessLoad;
    }

    /**
     * Find the new follower with less numb of partitions and the one which is not handling the given partition of the topic
     */
    public static Host findNewFollower(Partition partition) {
        Host follower = null;
        brokerLock.writeLock().lock();

        int min = Integer.MAX_VALUE;

        for (Host host : brokers) {
            if (host.isActive() && !partition.contains(host) && host.getNumberOfPartitions() < min) {
                follower = host;
                min = host.getNumberOfPartitions();
            }
        }

        if (follower != null) {
            follower.incrementNumOfPartitions();
            //Creating new object as further updates on the object should not affect original one
            follower = new Host(follower);
            follower.setDesignation(Constants.BROKER_DESIGNATION.FOLLOWER.getValue());
        }

        brokerLock.writeLock().unlock();
        return follower;
    }

    /**
     * Checks whether broker exists
     */
    public static boolean isBrokerExist(Host broker) {
        boolean flag;
        brokerLock.readLock().lock();

        flag = brokers.stream().anyMatch(host -> host.getAddress().equals(broker.getAddress()) && host.getPort() == broker.getPort());

        brokerLock.readLock().unlock();
        return flag;
    }

    /**
     * Get the broker object with the given address and port
     */
    public static Host getBroker(String address, int port) {
        Host broker;
        brokerLock.readLock().lock();

        broker = brokers.stream().filter(host -> host.getAddress().equals(address) && host.getPort() == port).findAny().orElse(null);

        brokerLock.readLock().unlock();
        return broker;
    }

    /**
     * Get the number of brokers in the network
     */
    public static int getNumberOfBrokers() {
        int size;
        brokerLock.readLock().lock();

        size = brokers.size();

        brokerLock.readLock().unlock();
        return size;
    }

    /**
     * Add new topic to the collection.
     */
    public static void addTopic(Topic topic) {
        topicLock.writeLock().lock();

        topicMap.put(topic.getName(), topic);

        for (Partition partition : topic.getPartitions()) {
            partitionMap.put(partition.getString(), partition);
        }

        topicLock.writeLock().unlock();
    }

    /**
     * Remove topic information from the collection
     */
    public static void removeTopic(Topic topic) {
        topicLock.writeLock().lock();

        topicMap.remove(topic.getName());

        for (Partition partition : topic.getPartitions()) {
            partitionMap.remove(String.format("%s:%s", partition.getTopicName(), partition.getNumber()));
        }

        topicLock.writeLock().unlock();
    }

    /**
     * Remove partitions of the give topic from the map
     */
    public static void removePartitions(Topic topicToRemove) {
        topicLock.writeLock().lock();

        Topic topic = topicMap.get(topicToRemove.getName());

        for (Partition partition : topicToRemove.getPartitions()) {
            partitionMap.remove(String.format("%s:%s", partition.getTopicName(), partition.getNumber()));

            topic.remove(partition.getNumber());
        }

        if (topic.getNumOfPartitions() == 0) {
            //No partitions for the topic left. Remove topic reference itself.
            topicMap.remove(topicToRemove.getName());
        }

        topicLock.writeLock().unlock();
    }

    /**
     * Make failed broker as inactive.
     * Remove failed broker from partition MD
     * Get new follower which will handle the partition of the topic
     */
    public static Topic updateMDAfterFailure(BrokerUpdateRequest request) {
        Host newFollower;
        topicLock.writeLock().lock();

        Partition partition = getPartition(request.getTopic(), request.getPartition());
        Host broker = getBroker(request.getBroker().getAddress(), request.getBroker().getPort());

        if (request.getBroker().isLeader()) {
            logger.debug("Leader is failed");
            partition.removeLeader();
        } else {
            logger.debug("Follower is failed");
            partition.removeFollower(broker);
        }

        broker.decrementNumOfPartitions();
        broker.setInActive();

        newFollower = findNewFollower(partition);
        logger.info(String.format("Broker %s:%d will handled topic %s:%d.", newFollower.getAddress(), newFollower.getPort(), partition.getTopicName(), partition.getNumber()));
        partition.addBroker(newFollower);

        topicLock.writeLock().unlock();
        return new Topic(partition.getTopicName(), partition);
    }

    /**
     * Update database to make the given broker as leader for the given partition of the topic
     */
    public static void updateLeader(BrokerUpdateRequest request) {
        topicLock.writeLock().lock();

        Partition partition = getPartition(request.getTopic(), request.getPartition());
        partition.setLeader(new Host(request.getBroker()));
        logger.debug(String.format("Set broker %s:%d leader of topic %s:%d.", request.getBroker().getAddress(), request.getBroker().getPort(), partition.getTopicName(), partition.getNumber()));

        topicLock.writeLock().unlock();
    }

    /**
     * Checks whether topic exist
     */
    public static boolean isTopicExist(String topic) {
        boolean flag;
        topicLock.readLock().lock();

        flag = topicMap.containsKey(topic);

        topicLock.readLock().unlock();
        return flag;
    }

    /**
     * Get the partition with the given topic name and partition number
     */
    public static Partition getPartition(String name, int partition) {
        Partition part;
        topicLock.readLock().lock();

        part = partitionMap.getOrDefault(String.format("%s:%s", name, partition), null);

        topicLock.readLock().unlock();
        return part;
    }

    /**
     * Checks whether partition with the given topic name and partition number exists or not
     */
    public static boolean isPartitionExist(String name, int partition) {
        boolean flag;
        topicLock.readLock().lock();

        flag = partitionMap.containsKey(String.format("%s:%s", name, partition));

        topicLock.readLock().unlock();
        return flag;
    }

    /**
     * Get the leader broker handling given topic and partition
     */
    public static Host getLeader(String name, int partitionNum) {
        Host leader;
        topicLock.readLock().lock();

        Partition partition = getPartition(name, partitionNum);
        leader = partition.getLeader();

        topicLock.readLock().unlock();
        return leader;
    }
}
