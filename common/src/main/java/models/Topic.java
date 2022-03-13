package models;

import utilities.Strings;

import java.util.List;

public class Topic extends Object {
    private String name;
    private int numOfPartitions;
    private List<Partition> partitions;

    public Topic(String name) {
        this.name = name;
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
}
