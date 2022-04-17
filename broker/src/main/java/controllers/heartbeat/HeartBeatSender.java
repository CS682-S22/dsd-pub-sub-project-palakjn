package controllers.heartbeat;

import configurations.BrokerConstants;
import controllers.Connection;
import controllers.HostService;
import controllers.Channels;
import models.HeartBeatRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Responsible for sending heartbeat messages
 *
 * @author Palak Jain
 */
public class HeartBeatSender {
    private static final Logger logger = LogManager.getLogger(HeartBeatSender.class);

    /**
     * Schedule the task to send heartbeat messages at a fixed interval
     */
    public boolean send(HeartBeatRequest request) {
        Connection connection = Channels.get(request.getReceivedId(), BrokerConstants.CHANNEL_TYPE.HEARTBEAT);

        if (connection == null || !connection.isOpen()) {
            connection = connect(request.getReceivedId());

            if (connection != null) {
                Channels.add(request.getReceivedId(), connection, BrokerConstants.CHANNEL_TYPE.HEARTBEAT);
            } else {
                return false;
            }
        }

        Runnable task = new HeartBeatTask(connection, request);
        HeartBeatSchedular.start(request.getKey(), task, BrokerConstants.HEARTBEAT_INTERVAL_MS);

        return true;
    }

    /**
     * Create the connection with another broker
     */
    private Connection connect(String receiveId) {
        Connection connection = null;
        String address;
        int port;

        String[] parts = receiveId.split(":");

        if (parts.length == 2) {
            address = parts[0];
            port = Integer.parseInt(parts[1]);

            HostService hostService = new HostService(logger);
            connection = hostService.connect(address, port);
        }

        return connection;
    }
}
