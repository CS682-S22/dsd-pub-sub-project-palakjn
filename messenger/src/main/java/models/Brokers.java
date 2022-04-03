package models;

import java.util.ArrayList;
import java.util.List;

/**
 * Responsible for maintaining a list of brokers information
 *
 * @author Palak Jain
 */
public class Brokers {
    private List<Host> brokers;

    public Brokers() {
        brokers = new ArrayList<>();
    }

    public Brokers(List<Host> brokers) {
        this.brokers = brokers;
    }

    /**
     * Adding new broker to the collection
     */
    public void add(Host broker) {
        brokers.add(broker);
    }

    /**
     * Checks if broker exists
     */
    public boolean contains(Host broker) {
        return brokers.stream().anyMatch(host -> host.getAddress().equals(broker.getAddress()) && host.getPort() == broker.getPort() && host.getPriorityNum() == broker.getPriorityNum());
    }

    /**
     * Gets the total number of brokers in the list
     */
    public int length() {
        return brokers.size();
    }

    /**
     * Get the brokers list
     */
    public List<Host> getBrokers() {
        return brokers;
    }

    /**
     * Finds the broker with the highest priority number
     */
    public Host getLeader() {
        Host leader = null;
        int max = -1;

        for (Host host : brokers) {
            if (host.getPriorityNum() > max) {
                leader = host;
                max = host.getPriorityNum();
            }
        }

        return leader;
    }

    /**
     * Represent the list of brokers in a String format
     */
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();

        for (Host host : brokers) {
            stringBuilder.append(String.format("%s:%d, ", host.getAddress(), host.getPort()));
        }

        return stringBuilder.toString();
    }
}
