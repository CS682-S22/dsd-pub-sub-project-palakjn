package controllers.replication;

import controllers.Connection;
import controllers.HostService;
import models.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Responsible for holding follower details and sending replica data.
 *
 * @author Palak Jain
 */
public class Follower extends Host {
    private static final Logger logger = LogManager.getLogger(Follower.class);
    private HostService hostService;
    private Connection connection;

    public Follower(Host host) {
        address = host.getAddress();
        port = host.getPort();
        hostService = new HostService(logger);
    }

    /**
     * Send replica data to the follower
     */
    public boolean send(byte[] data) {
        boolean isSuccess = false;

        if (connection == null || !connection.isOpen()) {
            connection = hostService.connect(address, port);
        }

        if (connection != null && connection.isOpen()) {
            isSuccess = hostService.sendPacketWithACK(connection, data, "REPLICA DATA");
        }

        return isSuccess;
    }

    /**
     * CLose the connection with the follower
     */
    public void close() {
        connection.closeConnection();
    }
}
