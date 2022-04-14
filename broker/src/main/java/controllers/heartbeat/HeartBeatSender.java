package controllers.heartbeat;

import controllers.Connection;
import controllers.HostService;
import controllers.connections.Channels;
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
        Connection connection = Channels.get(request.getKey());

        if (connection == null) {
            connection = connect(request.getReceivedId());

            if (connection != null) {
                Channels.add(request.getReceivedId(), connection);
            } else {
                return false;
            }
        }

        Runnable task = new HeartBeatTask(connection, request);
        HeartBeatSchedular.start(request.getKey(), task);

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
