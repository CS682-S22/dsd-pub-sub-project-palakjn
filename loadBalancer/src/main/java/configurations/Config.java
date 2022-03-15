package configurations;

import configuration.Constants;
import utilities.Strings;

public class Config {
    private String address;
    private int port;

    /**
     * @return Gets the address of the source host
     */
    public String getAddress() {
        return address;
    }

    /**
     * @return Get the port number of the source host at which it is listening
     */
    public int getPort() {
        return port;
    }
}
