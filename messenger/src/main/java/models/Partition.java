package models;

import java.util.List;

/**
 * Responsible for holding partition information
 *
 * @author Palak Jain
 */
public class Partition extends Object {
    private String topicName;
    private int number;
    private Host leader;
    private List<Host> brokers;

    public Partition(String topicName, int number, Host leader, List<Host> brokers) {
        this.topicName = topicName;
        this.number = number;
        this.leader = leader;
        this.brokers = brokers;
    }

    public Partition(String topicName, int number, Host leader) {
        this.topicName = topicName;
        this.number = number;
        this.leader = leader;
    }

    /**
     * Get the topic name of whose this partition is
     */
    public String getTopicName() {
        return topicName;
    }

    /**
     * Get the partition number
     */
    public int getNumber() {
        return number;
    }

    /**
     * Get the leader which is holding this partition
     */
    public Host getLeader() {
        return leader;
    }

    /**
     * Get all the brokers which are holding this partition
     */
    public List<Host> getBrokers() {
        return brokers;
    }

    /**
     * Add the broker to the list
     */
    public void addBroker(Host broker) {
        brokers.add(broker);
    }

    /**
     * Get the length of the brokers
     */
    public int getTotalBrokers() {
        return brokers.size();
    }

    /**
     * Get the broker object at the given index
     */
    public Host getBroker(int index) {
        Host broker = null;

        if (index < brokers.size()) {
            broker = brokers.get(index);
        }

        return broker;
    }

    /**
     * Get the topic name and partition number in a single string
     */
    public String getString() {
        return String.format("%s:%d", topicName, number);
    }
}
