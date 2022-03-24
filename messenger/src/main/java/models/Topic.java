package models;

import utilities.Strings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Responsible for holding topic information
 *
 * @author Palak Jain
 */
public class Topic extends Object {
    private String name;
    private int numOfPartitions;
    private List<Partition> partitions;

    public Topic(String name) {
        this.name = name;
        this.partitions = new ArrayList<>();
    }

    public Topic(String name, int numOfPartitions) {
        this.name = name;
        this.numOfPartitions = numOfPartitions;
        this.partitions = new ArrayList<>();
    }

    /**
     * Get the name of the topic
     */
    public String getName() {
        return name;
    }

    /**
     * Get the number of partitions
     */
    public int getNumOfPartitions() {
        return numOfPartitions;
    }

    /**
     * Get the partitions
     */
    public List<Partition> getPartitions() {
        return partitions;
    }

    /**
     * Add new partition to the list
     */
    public void addPartition(Partition partition) {
        if (partitions == null) {
            partitions = new ArrayList<>();
        }
        partitions.add(partition);
        numOfPartitions++;
    }

    /**
     * Checks whether the name is not null or empty
     */
    public boolean isValid() {
        return !Strings.isNullOrEmpty(name);
    }

    /**
     * Group topic by broker
     */
    public HashMap<String, Topic> groupBy() {
        HashMap<String, Topic> result = new HashMap<>();

        for (Partition partition : partitions) {
            String key = partition.getBroker().getString();
            if (!result.containsKey(key)) {
                result.put(key, new Topic(this.name));
            }

            result.get(key).addPartition(partition);
        }

        return result;
    }

    /**
     * Remove partition from the topic
     */
    public void remove(int partNum) {
        partitions.removeIf(partition -> partition.getNumber() == partNum);
        numOfPartitions--;
    }

    /**
     * Get the String containing list of partition identifiers topic has
     */
    public String getPartitionString() {
        StringBuilder builder = new StringBuilder();

        for (Partition partition : partitions) {
            builder.append(partition.getNumber());
            builder.append(" ,");
        }

        return builder.toString();
    }
}
