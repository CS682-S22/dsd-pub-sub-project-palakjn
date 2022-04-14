package controllers.heartbeat;

import controllers.Connection;
import models.HeartBeatRequest;
import utilities.BrokerPacketHandler;

/**
 * Responsible for creating the task to send heartbeat message to another broker.
 *
 * @author Palak Jain
 */
public class HeartBeatTask implements Runnable {
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
            HeartBeatSchedular.cancel(request.getKey());
        }
    }
}
