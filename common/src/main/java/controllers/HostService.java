package controllers;

import configuration.Constants;
import models.Header;
import org.apache.log4j.Logger;
import utilities.NodeTimer;
import utilities.PacketHandler;

public class HostService {
    private Logger logger;
    private Connection connection;

    public HostService(Connection connection, Logger logger) {
        this.connection = connection;
        this.logger = logger;
    }

    public void sendACK(Constants.REQUESTER requester, int seqNum) {
        byte[] acknowledgement = PacketHandler.createHeader(requester, Constants.TYPE.ACK, seqNum);
        connection.send(acknowledgement);
    }

    public void sendNACK(Constants.REQUESTER requester, int seqNum) {
        byte[] negAck = PacketHandler.createHeader(requester, Constants.TYPE.NACK, seqNum);
        connection.send(negAck);
    }

    public void sendNACK(Constants.REQUESTER requester) {
        byte[] negAck = PacketHandler.createHeader(requester, Constants.TYPE.NACK);
        connection.send(negAck);
    }

    public boolean sendPacketWithACK(byte[] packet, String packetName) {
        boolean isSuccess = false;
        NodeTimer timer = new NodeTimer();
        boolean running = true;

        connection.send(packet);
        timer.startTimer(packetName);

        while (running) {
            if (timer.isTimeout()) {
                logger.warn(String.format("[%s:%d] Time-out happen for the packet %s to the host %s:%d. Re-sending the packet.", connection.getSourceIPAddress(), connection.getSourcePort(), packetName, connection.getDestinationIPAddress(), connection.getDestinationPort()));
                connection.send(packet);
                timer.startTimer(packetName);
            } else if (connection.isAvailable()) {
                byte[] responseBytes = connection.receive();

                if (responseBytes != null) {
                    Header.Content header = PacketHandler.getHeader(responseBytes);

                    if (header != null) {
                        if (header.getType() == Constants.TYPE.ACK.getValue()) {
                            logger.info(String.format("[%s:%d] Received an acknowledgment for the %s request from the host %s:%d.", connection.getSourceIPAddress(), connection.getSourcePort(), packetName, connection.getDestinationIPAddress(), connection.getDestinationPort()));
                            timer.stopTimer();
                            running = false;
                            isSuccess = true;
                        } else if (header.getType() == Constants.TYPE.NACK.getValue()) {
                            logger.warn(String.format("[%s:%d] Received negative acknowledgment for the %s request from the host %s:%d. Not retrying.", connection.getSourceIPAddress(), connection.getSourcePort(), packetName, connection.getDestinationIPAddress(), connection.getDestinationPort()));
                            timer.stopTimer();
                            running = false;
                        }
                    }
                }
            }
        }

        return isSuccess;
    }
}
