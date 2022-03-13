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

    private CacheManager() {
        brokers = new ArrayList<>();
        topicMap = new HashMap<>();
        partitionMap = new HashMap<>();
    }

    public static boolean isExist(Host broker) {
        return brokers.stream().anyMatch(host -> host.getAddress().equals(broker.getAddress()) && host.getPort() == broker.getPort());
    }

    public static void addBroker(Host broker) {
        if (!isExist(broker)) {
            brokers.add(broker);
        }
    }

    public static List<Host> getBrokers() {
        return brokers;
    }

    public static void removeBroker(Host broker) {
        brokers.removeIf(host -> host.getAddress().equals(broker.getAddress()) && host.getPort() == broker.getPort());
    }

    public static boolean isTopicExist(String topic) {
        return topicMap.containsKey(topic);
    }

    public static void addTopic(Topic topic) {
        if (!topicMap.containsKey(topic.getName())) {
            topicMap.put(topic.getName(), topic);

            for (Partition partition : topic.getPartitions()) {
                partitionMap.put(String.format("%s_%s", partition.getTopicName(), partition.getNumber()), partition);
            }
        }
    }

    public static Topic getTopic(String name) {
        return topicMap.getOrDefault(name, null);
    }

    public static Partition getPartition(String name, int partition) {
        return partitionMap.getOrDefault(String.format("%s_%s", name, partition), null);
    }

    public static boolean isPartitionExist(String name, int partition) {
        return partitionMap.containsKey(String.format("%s_%s", name, partition));
    }
}
