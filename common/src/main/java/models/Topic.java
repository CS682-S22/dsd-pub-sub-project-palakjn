package models;

import utilities.Strings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Topic extends Object {
    private String name;
    private int numOfPartitions;
    private List<Partition> partitions;

    public Topic(String name) {
        this.name = name;
        this.partitions = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public int getNumOfPartitions() {
        return numOfPartitions;
    }

    public List<Partition> getPartitions() {
        return partitions;
    }

    public void addPartition(Partition partition) {
        this.partitions.add(partition);
        this.numOfPartitions++;
    }

    public boolean isValid() {
        return !Strings.isNullOrEmpty(name);
    }

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

    public String getPartitionString() {
        StringBuilder builder = new StringBuilder();

        for (Partition partition : partitions) {
            builder.append(partition.getNumber());
            builder.append(" ,");
        }

        return builder.toString();
    }
}
