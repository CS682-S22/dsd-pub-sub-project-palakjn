package controllers;

import configuration.Constants;
import models.Header;
import org.apache.logging.log4j.Logger;
import utilities.NodeTimer;
import utilities.PacketHandler;

/**
 * Responsible for handling common services which a host needs like sending ACK, NACK, etc.
 *
 * @author Palak Jain
 */
public class HostService {
    private Logger logger;

    public HostService(Logger logger) {
        this.logger = logger;
    }

    /**
     * Send an acknowledgment response to the host
     */
    public void sendACK(Connection connection, Constants.REQUESTER requester, int seqNum) {
        byte[] acknowledgement = PacketHandler.createACK(requester, seqNum);
        connection.send(acknowledgement);
    }

    /**
     * Send the negative acknowledgment response with the sequence number to the host
     */
    public void sendNACK(Connection connection, Constants.REQUESTER requester, int seqNum) {
        byte[] negAck = PacketHandler.createNACK(requester, seqNum);
        connection.send(negAck);
    }

    /**
     * Send the negative acknowledgment response to the host
     */
    public void sendNACK(Connection connection, Constants.REQUESTER requester) {
        byte[] negAck = PacketHandler.createNACK(requester);
        connection.send(negAck);
    }

    /**
     * Send the packet to the host and waits for an acknowledgement. Re-send the packet if time-out
     */
    public boolean sendPacketWithACK(Connection connection, byte[] packet, String packetName) {
        boolean isSuccess = false;
        NodeTimer timer = new NodeTimer();
        boolean running = true;

        connection.send(packet);
        timer.startTimer(packetName, Constants.RTT);

        while (running) {
            if (timer.isTimeout()) {
                logger.warn(String.format("[%s:%d] Time-out happen for the packet %s to the host. Re-sending the packet.", connection.getDestinationIPAddress(), connection.getDestinationPort(), packetName));
                connection.send(packet);
                timer.stopTimer();
                timer.startTimer(packetName, Constants.RTT);
            } else if (connection.isAvailable()) {
                byte[] responseBytes = connection.receive();

                if (responseBytes != null) {
                    Header.Content header = PacketHandler.getHeader(responseBytes);

                    if (header != null) {
                        if (header.getType() == Constants.TYPE.ACK.getValue()) {
                            logger.info(String.format("[%s:%d] Received an acknowledgment for the %s request from the host.", connection.getDestinationIPAddress(), connection.getDestinationPort(), packetName));
                            timer.stopTimer();
                            running = false;
                            isSuccess = true;
                        } else if (header.getType() == Constants.TYPE.NACK.getValue()) {
                            logger.warn(String.format("[%s:%d] Received negative acknowledgment for the %s request from the host. Not retrying.", connection.getDestinationIPAddress(), connection.getDestinationPort(), packetName));
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