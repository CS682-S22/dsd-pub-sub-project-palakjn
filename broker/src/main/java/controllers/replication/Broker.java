package controllers.replication;

import configurations.BrokerConstants;
import controllers.Connection;
import controllers.HostService;
import controllers.connections.Channels;
import models.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Responsible for holding follower details and sending replica data.
 *
 * @author Palak Jain
 */
public class Broker extends Host {
    private static final Logger logger = LogManager.getLogger(Broker.class);
    private HostService hostService;

    public Broker(Host host) {
        super(host);
        hostService = new HostService(logger);
    }

    /**
     * Send replica data to the follower
     */
    public boolean send(byte[] data, BrokerConstants.CHANNEL_TYPE channel_type, int waitTime, boolean retry) {
        boolean isSuccess = false;

        Connection connection = Channels.get(getString(), channel_type);

        if (connection == null || !connection.isOpen()) {
            connection = hostService.connect(address, port);
        }

        if (connection != null && connection.isOpen()) {
            Channels.upsert(getString(), connection, channel_type);
            isSuccess = hostService.sendPacketWithACK(connection, data, waitTime, retry);
        }

        return isSuccess;
    }

    /**
     * CLose the connection with the follower
     */
    public void close() {
        Connection connection = Channels.get(getString(), BrokerConstants.CHANNEL_TYPE.DATA);
        connection.closeConnection();

        Channels.remove(getString(), BrokerConstants.CHANNEL_TYPE.DATA);
    }
}