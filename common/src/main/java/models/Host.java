package models;

import configuration.Constants;
import utilities.Strings;

public class Host extends Object {
    private String address;
    private int port;
    private int numberOfPartitions; // Number of partitions a corresponding broker holding. [Will be 0 for other type of hosts: producer/consumer/load balancer]

    public Host(String address, int port) {
        this.address = address;
        this.port = port;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public int getNumberOfPartitions() {
        return numberOfPartitions;
    }

    public void incrementNumOfPartitions() {
        this.numberOfPartitions++;
    }

    public boolean isValid() {
        return !(Strings.isNullOrEmpty(address) || port < Constants.START_VALID_PORT || port > Constants.END_VALID_PORT);
    }
}
