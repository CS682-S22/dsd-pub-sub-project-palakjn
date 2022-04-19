package models;

import com.google.gson.annotations.Expose;
import configuration.Constants;

import java.util.ArrayList;
import java.util.List;

/**
 * Responsible for holding partition information
 *
 * @author Palak Jain
 */
public class Partition extends Object {
    @Expose
    private String topicName;
    @Expose
    private int number;
    @Expose
    private Host leader;
    @Expose
    private List<Host> brokers;

    public Partition(String topicName, int number, Host leader, List<Host> brokers) {
        this.topicName = topicName;
        this.number = number;
        this.leader = leader;
        this.brokers = new ArrayList<>(brokers);
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
     * Set the new leader for the partition of the topic
     */
    public void setLeader(Host leader) {
        this.leader = leader;
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
        brokers.add(new Host(broker.getAddress(), broker.getPort()));
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

    /**
     * Set leader of the partition as inactive
     */
    public void removeLeader() {
        leader.setInActive();
        brokers.removeIf(broker -> broker.getDesignation() == Constants.BROKER_DESIGNATION.LEADER.getValue());
    }

    /**
     * Remove the follower
     */
    public void removeFollower(Host brokerToRemove) {
        brokers.removeIf(broker -> broker.getAddress().equals(brokerToRemove.getAddress()) && broker.getPort() == brokerToRemove.getPort());
    }

    /**
     * Checks if the broker handling the current partition
     */
    public boolean contains(Host broker) {
        return brokers.stream().anyMatch(host -> host.getAddress().equals(broker.getAddress()) && host.getPort() == broker.getPort());
    }
}
