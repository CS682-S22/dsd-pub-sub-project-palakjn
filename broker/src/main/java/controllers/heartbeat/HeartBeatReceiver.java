package controllers.heartbeat;

import configurations.BrokerConstants;
import controllers.Connection;
import controllers.Channels;
import models.heartbeat.HeartBeatRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.BrokerPacketHandler;
import utilities.JSONDesrializer;

/**
 * Responsible for receiving heartbeat messages.
 *
 * @author Palak Jain
 */
public class HeartBeatReceiver {
    private static final Logger logger = LogManager.getLogger(HeartBeatReceiver.class);
    private FailureDetector failureDetector;

    public HeartBeatReceiver() {
        failureDetector = new FailureDetector();
    }

    /**
     * Receive the heartbeat message from the broker
     */
    public void handleRequest(Connection connection, byte[] request) {
        byte[] data = BrokerPacketHandler.getData(request);

        if (data != null) {
            HeartBeatRequest heartBeatRequest = JSONDesrializer.fromJson(data, HeartBeatRequest.class);
            //Adding connection to the channel to be re-use
            Channels.add(heartBeatRequest.getServerId(), connection, BrokerConstants.CHANNEL_TYPE.HEARTBEAT);
            failureDetector.heartBeatReceived(heartBeatRequest.getKey(), heartBeatRequest.getServerId());
        } else {
            logger.warn(String.format("[%s:%d] Unable to parse body from the received packet from another host %s:%d. ", connection.getSourceIPAddress(), connection.getSourcePort(), connection.getDestinationIPAddress(), connection.getDestinationPort()));
        }
    }
}
