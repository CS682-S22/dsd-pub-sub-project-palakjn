package models;

import configuration.Constants;
import utilities.Strings;

/**
 * Responsible for holding host information
 *
 * @author Palak Jain
 */
public class Host extends Object {
    private String address;
    private int port;
    private int numberOfPartitions; // Number of partitions a corresponding broker holding. [Will be 0 for other type of hosts: producer/consumer/load balancer]

    public Host(String address, int port) {
        this.address = address;
        this.port = port;
    }

    public Host() {

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

}
