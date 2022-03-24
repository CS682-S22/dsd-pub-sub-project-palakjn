package controllers;

import utilities.BrokerPacketHandler;

/**
 * Responsible for maintaining the subscriber information.
 *
 * @author Palak Jain
 */
public class Subscriber implements ISubscriber {
    private Connection connection;

    public Subscriber(Connection connection) {
        this.connection = connection;
    }

    /**
     * Gets the address of the subscriber
     */
    public String getAddress() {
        return String.format("%s:%d", connection.getDestinationIPAddress(), connection.getDestinationPort());
    }

    /**
     * Send the data to the subscriber
     */
    @Override
    public synchronized void onEvent(byte[] data) {
        byte[] packet = BrokerPacketHandler.createDataPacket(data);
        connection.send(packet);
    }
}
