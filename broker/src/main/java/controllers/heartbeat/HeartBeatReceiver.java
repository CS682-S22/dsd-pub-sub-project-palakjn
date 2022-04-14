package controllers.heartbeat;

import configurations.BrokerConstants;
import configurations.Config;
import controllers.Connection;
import controllers.connections.Channels;
import models.Header;
import models.HeartBeatRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.BrokerPacketHandler;
import utilities.JSONDesrializer;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Responsible for receiving heartbeat messages.
 *
 * @author Palak Jain
 */
public class HeartBeatReceiver {
    private static final Logger logger = LogManager.getLogger(HeartBeatReceiver.class);
    private ExecutorService threadPool;
    private boolean running;

    public HeartBeatReceiver() {
        threadPool = Executors.newFixedThreadPool(BrokerConstants.MAX_HEARTBEAT_TASKS);
        running = true;
    }

    /**
     * Listen for connections from another brokers to get heartbeat messages
     */
    public void listen(Config config) {
        ServerSocket serverSocket;

        try {
            serverSocket = new ServerSocket(config.getHeartBeatServer().getPort());
        } catch (IOException exception) {
            logger.error(String.format("Fail to start the broker to receive heartbeat messages at the node %s:%d.", config.getHeartBeatServer().getAddress(), config.getHeartBeatServer().getPort()));

            return;
        }

        logger.info(String.format("[%s] Listening on port %d.", config.getHeartBeatServer().getAddress(), config.getHeartBeatServer().getPort()));

        while (running) {
            try {
                Socket socket = serverSocket.accept();
                logger.info(String.format("[%s:%d] Received the connection from the host.", socket.getInetAddress().getHostAddress(), socket.getPort()));
                Connection connection = new Connection(socket, socket.getInetAddress().getHostAddress(), socket.getPort(), config.getHeartBeatServer().getAddress(), config.getHeartBeatServer().getPort());
                if (connection.openConnection()) {
                    threadPool.execute(() -> handleRequest(connection));
                }
            } catch (IOException exception) {
                logger.error(String.format("[%s:%d] Fail to accept the connection from another host. ", config.getHeartBeatServer().getAddress(), config.getHeartBeatServer().getPort()), exception);
            }
        }
    }

    /**
     * Receive the heartbeat message from the broker
     */
    private void handleRequest(Connection connection) {
        while (connection.isOpen()) {
            byte[] request = connection.receive();

            if (request != null) {
                Header.Content header = BrokerPacketHandler.getHeader(request);

                if (header != null) {
                    if (header.getType() == BrokerConstants.TYPE.HEARTBEAT.getValue()) {
                        byte[] data = BrokerPacketHandler.getData(request);

                        if (data != null) {
                            HeartBeatRequest heartBeatRequest = JSONDesrializer.fromJson(data, HeartBeatRequest.class);
                            //Adding connection to the channel to be re-use
                            Channels.add(heartBeatRequest.getServerId(), connection, BrokerConstants.CHANNEL_TYPE.HEARTBEAT);
                            //TODO: failureDetector.heartBeatReceived(heartBeatRequest.getKey(), heartBeatRequest,getServerId());
                        } else {
                            logger.warn(String.format("[%s:%d] Unable to parse body from the received packet from another host %s:%d. ", connection.getSourceIPAddress(), connection.getSourcePort(), connection.getDestinationIPAddress(), connection.getDestinationPort()));
                        }
                    } else {
                        logger.warn(String.format("[%s:%d] Invalid packet %d received from another host %s:%d. ", connection.getSourceIPAddress(), connection.getSourcePort(), header.getType(), connection.getDestinationIPAddress(), connection.getDestinationPort()));
                    }
                } else {
                    logger.warn(String.format("[%s:%d] Unable to parse header from the received packet from another host %s:%d. ", connection.getSourceIPAddress(), connection.getSourcePort(), connection.getDestinationIPAddress(), connection.getDestinationPort()));
                }
            } else {
                logger.warn(String.format("[%s:%d] No heartbeat received from the host %s:%d. Connection might get closed. ", connection.getSourceIPAddress(), connection.getSourcePort(), connection.getDestinationIPAddress(), connection.getDestinationPort()));
            }
        }
    }
}
