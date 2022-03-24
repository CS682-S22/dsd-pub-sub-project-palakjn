package models;

/**
 * Responsible for holding partition information
 *
 * @author Palak Jain
 */
public class Partition extends Object {
    private String topicName;
    private int number;
    private Host broker;

    public Partition(String topicName, int number, Host broker) {
        this.topicName = topicName;
        this.number = number;
        this.broker = broker;
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
     * Get the broker which is holding this partition
     */
    public Host getBroker() {
        return broker;
    }

    /**
     * Get the topic name and partition number in a single string
     */
    public String getString() {
        return String.format("%s:%d", topicName, number);
    }
}
