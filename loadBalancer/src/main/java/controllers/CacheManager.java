package controllers;

import models.Host;
import models.Partition;
import models.Topic;

import java.util.*;

/**
 * Static global cache alike class storing the configuration of the system.
 *
 * @author Palak Jain
 */
public class CacheManager {
    private static List<Host> brokers;
    private static Map<String, Topic> topicMap;            // Map topic name to the topic object. Useful when a customer need to read from all the partitions of a topic
    private static Map<String, Partition> partitionMap;    // Map topic name + partition to the partition object. Useful when a customer wants to read from particular topic partition as well as when the producer wants to publish message

    //locks
    private static final Object brokerLock = new Object();
    private static final Object topicLock = new Object();

    private CacheManager() {
        brokers = new ArrayList<>();
        topicMap = new HashMap<>();
        partitionMap = new HashMap<>();
    }

    public static boolean isExist(Host broker) {
        synchronized (brokerLock) {
            return brokers.stream().anyMatch(host -> host.getAddress().equals(broker.getAddress()) && host.getPort() == broker.getPort());
        }
    }

    public static void addBroker(Host broker) {
        synchronized (brokerLock) {
            if (!isExist(broker)) {
                brokers.add(broker);
            }
        }
    }

    public static int getNumberOfBrokers() {
        synchronized (brokerLock) {
            return brokers.size();
        }
    }

    public static void removeBroker(Host broker) {
        synchronized (brokerLock) {
            brokers.removeIf(host -> host.getAddress().equals(broker.getAddress()) && host.getPort() == broker.getPort());
        }
    }

    public static Host findBrokerWithLessLoad() {
        synchronized (brokerLock) {
            Host broker = brokers.get(0);
            int min = broker.getNumberOfPartitions();

            for (int index = 1; index < brokers.size(); index++) {
                if (brokers.get(index).getNumberOfPartitions() < min) {
                    broker = brokers.get(index);
                    min = broker.getNumberOfPartitions();
                }
            }

            broker.incrementNumOfPartitions();
            return broker;
        }
    }

    public static boolean isTopicExist(String topic) {
        synchronized (topicLock) {
            return topicMap.containsKey(topic);
        }
    }

    public static boolean addTopic(Topic topic) {
        synchronized (topicLock) {
            boolean flag = false;

            if (!topicMap.containsKey(topic.getName())) {
                topicMap.put(topic.getName(), topic);

                for (Partition partition : topic.getPartitions()) {
                    partitionMap.put(partition.getString(), partition);
                }

                flag = true;
            }

            return flag;
        }
    }

    public static Topic getTopic(String name) {
        synchronized (topicLock) {
            return topicMap.getOrDefault(name, null);
        }
    }

    public static Partition getPartition(String name, int partition) {
        synchronized (topicLock) {
            return partitionMap.getOrDefault(String.format("%s:%s", name, partition), null);
        }
    }

    public static boolean isPartitionExist(String name, int partition) {
        synchronized (topicLock) {
            return partitionMap.containsKey(String.format("%s:%s", name, partition));
        }
    }
}
