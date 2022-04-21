package controllers;

import configurations.BrokerConstants;
import controllers.database.CacheManager;
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
    private volatile boolean isOutOfSync;
    private volatile int nextOffset;

    public Broker(Host host) {
        super(host);
        hostService = new HostService(logger);
    }

    public Broker(String serverId) {
        String[] parts = serverId.split(":");

        if (parts.length == 2) {
            address = parts[0];
            port = Integer.parseInt(parts[1]);
        }
    }

    /**
     * Determines whether the broker is out of sync for the topic
     */
    public boolean isOutOfSync() {
        return isOutOfSync;
    }

    /**
     * Set whether the broker is out of sync or nor
     */
    public void setOutOfSync(boolean outOfSync) {
        isOutOfSync = outOfSync;
    }

    /**
     * Set the last offset of the partition that broker hold
     */
    public void setNextOffsetToRead(int lastOffset) {
        this.nextOffset = lastOffset;
    }

    /**
     * Get the last offset of the partition that broker hold
     */
    public int getNextOffset() {
        return nextOffset;
    }

    /**
     * Send the packet to the broker and waits for an acknowledgement. Return true if success else false
     */
    public boolean send(byte[] data, BrokerConstants.CHANNEL_TYPE channel_type, int waitTime, boolean retry) {
        boolean isSuccess = false;

        Connection connection = getConnection(channel_type);

        if (connection != null) {
            isSuccess = hostService.sendPacketWithACK(connection, data, waitTime, retry);

            if (isSuccess) {
                logger.info(String.format("[%s:%d] Send %d bytes of data to the broker %s:%d.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort(), data.length, address, port));
            }
        }

        return isSuccess;
    }

    /**
     * Send the packet to the broker and waits for the response. Return the response if successful else null
     */
    public byte[] sendAndGetResponse(byte[] data, BrokerConstants.CHANNEL_TYPE channel_type, int waitTime, boolean retry) {
        Connection connection = getConnection(channel_type);
        byte[] response = null;

        if (connection != null && connection.isOpen()) {
            response = hostService.sendPacket(connection, data, waitTime, retry);
        }

        return response;
    }

    /**
     * Get the connection object
     */
    private Connection getConnection(BrokerConstants.CHANNEL_TYPE channel_type) {
        Connection connection = Channels.get(getString(), channel_type);

        if (connection == null || !connection.isOpen()) {
            connection = hostService.connect(address, port);
        }

        if (connection != null && connection.isOpen()) {
            connection = hostService.connect(address, port);
        }

        if (connection == null || !connection.isOpen()) {
            connection = null;
        } else {
            Channels.upsert(getString(), connection, channel_type);
        }

        return connection;
    }

    /**
     * CLose the connection with the follower
     */
    public void close() {
        Connection connection = Channels.get(getString(), BrokerConstants.CHANNEL_TYPE.DATA);
        if (connection != null) {
            connection.closeConnection();
            Channels.remove(getString(), BrokerConstants.CHANNEL_TYPE.DATA);
        }
    }
}
