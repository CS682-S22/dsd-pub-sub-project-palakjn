package models;

import com.google.gson.annotations.Expose;
import configuration.Constants;
import utilities.Strings;

/**
 * Responsible for holding host information
 *
 * @author Palak Jain
 */
public class Host extends Object {
    @Expose
    private int priorityNum;
    @Expose
    protected String address;
    @Expose
    protected int port;
    @Expose
    protected int heartBeatPort;
    @Expose
    private int designation; // 0 for leader and 1 for follower
    @Expose
    private String status; //ACTIVE or INACTIVE

    private int numberOfPartitions; // Number of partitions a corresponding broker holding. [Will be 0 for other type of hosts: producer/consumer/load balancer]

    public Host(int priorityNum, String address, int port, int heartBeatPort) {
        this.priorityNum = priorityNum;
        this.address = address;
        this.port = port;
        this.heartBeatPort = heartBeatPort;
        status = Constants.BROKER_STATUS.ACTIVE;
    }

    public Host(String address, int port) {
        this.address = address;
        this.port = port;
        status = Constants.BROKER_STATUS.ACTIVE;
    }

    public Host(Host host) {
         priorityNum = host.getPriorityNum();
         address = host.getAddress();
         port = host.getPort();
         heartBeatPort = host.getHeartBeatPort();
         status = Constants.BROKER_STATUS.ACTIVE;
    }

    public Host() {
        status = Constants.BROKER_STATUS.ACTIVE;
    }

    /**
     * Setting the priority number for the host
     */
    public void setPriorityNum(int priorityNum) {
        this.priorityNum = priorityNum;
    }

    /**
     * Getting the priority number of the host
     */
    public int getPriorityNum() {
        return priorityNum;
    }

    /**
     * Get the address of the host
     */
    public String getAddress() {
        return address;
    }

    /**
     * Get the port number
     */
    public int getPort() {
        return port;
    }

    /**
     * Get the number of partitions which the broker holding
     */
    public int getNumberOfPartitions() {
        return numberOfPartitions;
    }

    /**
     * Increment the number of partitions by 1
     */
    public void incrementNumOfPartitions() {
        this.numberOfPartitions++;
    }

    /**
     * Decrement the number of partitions by 1
     */
    public void decrementNumOfPartitions() {
        this.numberOfPartitions--;
    }

    /**
     * Get the heartbeat port of the host
     */
    public int getHeartBeatPort() {
        return heartBeatPort;
    }

    /**
     * Get whether the host is leader or follower
     */
    public int getDesignation() {
        return designation;
    }

    /**
     * Set whether the host is leader or follower
     */
    public void setDesignation(int designation) {
        this.designation = designation;
    }

    /**
     * Validates if the address is not null and port is in accepted range
     */
    public boolean isValid() {
        return !(Strings.isNullOrEmpty(address) || port < Constants.START_VALID_PORT || port > Constants.END_VALID_PORT);
    }

    /**
     * Get the address and port as one string
     */
    public String getString() {
        return String.format("%s:%d", address, port);
    }

    /**
     * Get the address and heartbeat port as one string
     */
    public String getHeartBeatString() {
        return String.format("%s:%d", address, heartBeatPort);
    }

    /**
     * Finds if the broker is leader
     */
    public boolean isLeader() {
        return designation == Constants.BROKER_DESIGNATION.LEADER.getValue();
    }

    /**
     * Set the broker as active
     */
    public void setActive() {
        status = Constants.BROKER_STATUS.ACTIVE;
    }

    /**
     * Set the broker as inactive
     */
    public void setInActive() {
        status = Constants.BROKER_STATUS.INACTIVE;
    }

    /**
     * Checks whether the host is active or inactive
     */
    public boolean isActive() {
        return status.equals(Constants.BROKER_STATUS.ACTIVE);
    }

    /**
     * Check whether caller and given host is equal
     */
    public boolean isSame(Host host) {
        return address.equals(host.getAddress()) && port == host.getPort();
    }
}
