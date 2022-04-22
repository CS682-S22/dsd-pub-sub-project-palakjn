package controllers.heartbeat;

import configurations.BrokerConstants;
import controllers.Connection;
import controllers.Channels;
import controllers.database.CacheManager;
import models.heartbeat.HeartBeatRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.BrokerPacketHandler;

/**
 * Responsible for creating the task to send heartbeat message to another broker.
 *
 * @author Palak Jain
 */
public class HeartBeatTask implements Runnable {
    private static final Logger logger = LogManager.getLogger(HeartBeatTask.class);
    HeartBeatRequest request;
    Connection connection;

    public HeartBeatTask(Connection connection, HeartBeatRequest request) {
        this.connection = connection;
        this.request = request;
    }

    @Override
    public void run() {
        if (connection.isOpen()) {
            byte[] packet = BrokerPacketHandler.createHeartBeatPacket(request);
            connection.send(packet);
        } else {
            HeartBeatSchedular.cancel(String.format("HeartBeatSend_%s_%s", request.getKey(), request.getReceivedId()));
            Channels.remove(request.getReceivedId(), BrokerConstants.CHANNEL_TYPE.HEARTBEAT);
        }
    }
}
