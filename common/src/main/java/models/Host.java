package models;

import configuration.Constants;
import utilities.Strings;

public class Host extends Object {
    private String address;
    private int port;

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

    public boolean isValid() {
        return !(Strings.isNullOrEmpty(address) || port < Constants.START_VALID_PORT || port > Constants.END_VALID_PORT);
    }
}
