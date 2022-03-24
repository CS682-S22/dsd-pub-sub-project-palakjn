package controllers;

import models.Host;
import models.Partition;
import models.Topic;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Static global cache alike class storing the configuration of the system.
 *
 * @author Palak Jain
 */
public class CacheManager {
    private static List<Host> brokers = new ArrayList<>();
    private static Map<String, Topic> topicMap = new HashMap<>();            // Map topic name to the topic object. Useful when a customer need to read from all the partitions of a topic
    private static Map<String, Partition> partitionMap = new HashMap<>();    // Map topic name + partition to the partition object. Useful when a customer wants to read from particular topic partition as well as when the producer wants to publish message

    //locks
    private static final ReentrantReadWriteLock brokerLock = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock topicLock = new ReentrantReadWriteLock();

    private CacheManager() {
    }

    public static void addBroker(Host broker) {
        brokerLock.writeLock().lock();

        if (!isExist(broker)) {
            brokers.add(broker);
        }

        brokerLock.writeLock().unlock();
    }

    public static void removeBroker(Host broker) {
        brokerLock.writeLock().lock();
        brokers.removeIf(host -> host.getAddress().equals(broker.getAddress()) && host.getPort() == broker.getPort());
        brokerLock.writeLock().unlock();
    }

    public static Host findBrokerWithLessLoad() {
        Host broker;
        brokerLock.writeLock().lock();

        broker = brokers.get(0);
        int min = broker.getNumberOfPartitions();

        for (int index = 1; index < brokers.size(); index++) {
            if (brokers.get(index).getNumberOfPartitions() < min) {
                broker = brokers.get(index);
                min = broker.getNumberOfPartitions();
            }
        }

        broker.incrementNumOfPartitions();

        brokerLock.writeLock().unlock();
        return broker;
    }

    public static boolean isBrokerExist(Host broker) {
        boolean flag;
        brokerLock.readLock().lock();

        flag = brokers.stream().anyMatch(host -> host.getAddress().equals(broker.getAddress()) && host.getPort() == broker.getPort());

        brokerLock.readLock().unlock();
        return flag;
    }

    public static int getNumberOfBrokers() {
        int size;
        brokerLock.readLock().lock();

        size = brokers.size();

        brokerLock.readLock().unlock();
        return size;
    }

    public static boolean isExist(Host broker) {
        boolean flag;
        brokerLock.readLock().lock();

        flag = brokers.stream().anyMatch(host -> host.getAddress().equals(broker.getAddress()) && host.getPort() == broker.getPort());

        brokerLock.readLock().unlock();
        return flag;
    }

    public static boolean addTopic(Topic topic) {
        boolean flag = false;
        topicLock.writeLock().lock();

        if (!topicMap.containsKey(topic.getName())) {
            topicMap.put(topic.getName(), topic);

            for (Partition partition : topic.getPartitions()) {
                partitionMap.put(partition.getString(), partition);
            }

            flag = true;
        }

        topicLock.writeLock().unlock();
        return flag;
    }

    public static void removeTopic(Topic topic) {
        topicLock.writeLock().lock();

        topicMap.remove(topic.getName());

        for (Partition partition : topic.getPartitions()) {
            partitionMap.remove(String.format("%s:%s", partition.getTopicName(), partition.getNumber()));
        }

        topicLock.writeLock().unlock();
    }

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

    public static boolean isTopicExist(String topic) {
        boolean flag;
        topicLock.readLock().lock();

        flag = topicMap.containsKey(topic);

        topicLock.readLock().unlock();
        return flag;
    }

    public static Topic getTopic(String name) {
        Topic topic;
        topicLock.readLock().lock();

        topic = topicMap.getOrDefault(name, null);

        topicLock.readLock().unlock();
        return topic;
    }

    public static Partition getPartition(String name, int partition) {
        Partition part;
        topicLock.readLock().lock();

        part = partitionMap.getOrDefault(String.format("%s:%s", name, partition), null);

        topicLock.readLock().unlock();
        return part;
    }

    public static boolean isPartitionExist(String name, int partition) {
        boolean flag;
        topicLock.readLock().lock();

        flag = partitionMap.containsKey(String.format("%s:%s", name, partition));

        topicLock.readLock().unlock();
        return flag;
    }
}
