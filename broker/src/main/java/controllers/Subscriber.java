package controllers;

import utilities.BrokerPacketHandler;

public class Subscriber implements ISubscriber {
    private Connection connection;

    public Subscriber(Connection connection) {
        this.connection = connection;
    }

    public String getAddress() {
        return String.format("%s:%d", connection.getDestinationIPAddress(), connection.getDestinationPort());
    }

    @Override
    public synchronized void onEvent(byte[] data) {
        byte[] packet = BrokerPacketHandler.createDataPacket(data);
        connection.send(packet);
    }
}
