package controllers.heartbeat;

import configurations.BrokerConstants;
import controllers.Connection;
import controllers.HostService;
import controllers.Channels;
import controllers.database.CacheManager;
import models.heartbeat.HeartBeatRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Responsible for sending heartbeat messages
 *
 * @author Palak Jain
 */
public class HeartBeatSender {
    private HostService hostService;
    private static final Logger logger = LogManager.getLogger(HeartBeatSender.class);

    public HeartBeatSender() {
        hostService = new HostService(logger);
    }

    /**
     * Schedule the task to send heartbeat messages at a fixed interval
     */
    public boolean send(HeartBeatRequest request) {

        logger.info(String.format("[%s] Making connection with the host %s.", CacheManager.getBrokerInfo().getString(), request.getReceivedId()));
        Connection connection = connect(request.getReceivedId());

        if (connection != null) {
            logger.info(String.format("[%s] Start sending heartbeat messages to the server id %s for key %s.", CacheManager.getBrokerInfo().getString(), request.getReceivedId(), request.getKey()));
            Runnable task = new HeartBeatTask(connection, request);
            HeartBeatSchedular.start(request.getKey(), task, BrokerConstants.HEARTBEAT_INTERVAL_MS);

            return true;
        }

        return false;
    }

    /**
     * Create the connection with another broker
     */
    private synchronized Connection connect(String receiveId) {
        Connection connection = Channels.get(receiveId, BrokerConstants.CHANNEL_TYPE.HEARTBEAT);;

        if (connection == null || !connection.isOpen()) {
            String address;
            int port;

            String[] parts = receiveId.split(":");

            if (parts.length == 2) {
                address = parts[0];
                port = Integer.parseInt(parts[1]);

                connection = hostService.connect(address, port);

                if (connection != null && connection.isOpen()) {
                    Channels.add(receiveId, connection, BrokerConstants.CHANNEL_TYPE.HEARTBEAT);
                } else {
                    connection = null;
                }
            } else {
                connection = null;
            }
        }

        return connection;
    }
}
