package controllers;

import models.File;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CacheManager {
    private static Map<String, File> partitions;
    private static List<String> topics;

    private CacheManager() {
        partitions = new HashMap<>();
        topics = new ArrayList<>();
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
}
