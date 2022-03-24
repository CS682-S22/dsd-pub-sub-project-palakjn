package configurations;

import configuration.Constants;
import utilities.Strings;

/**
 Responsible for holding load balancer config values

 @author Palak Jain
 */
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

    public boolean isValid() {
        return !Strings.isNullOrEmpty(address) && port >= Constants.START_VALID_PORT && port <= Constants.END_VALID_PORT;
    }
}
