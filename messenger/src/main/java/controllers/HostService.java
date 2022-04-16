package controllers;

import configuration.Constants;
import models.Header;
import org.apache.logging.log4j.Logger;
import utilities.PacketHandler;

import java.io.IOException;
import java.net.Socket;

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
     * Open connection with the host
     */
    public Connection connect(String address, int port) {
        Connection connection = null;

        try {
            Socket socket = new Socket(address, port);
            logger.info(String.format("[%s:%d] Successfully connected to the broker.", address, port));

            connection = new Connection(socket, address, port);
            if (!connection.openConnection()) {
                connection = null;
            }
        } catch (IOException exception) {
            logger.error(String.format("[%s:%d] Fail to make connection with the host.", address, port), exception.getMessage());
        }

        return connection;
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
    public boolean sendPacketWithACK(Connection connection, byte[] packet, int timeout) {
        return sendPacketWithACK(connection, packet, timeout, true);
    }

    /**
     * Send the packet to the host and waits for an acknowledgement. Re-send the packet if time-out only if "retry" param is set to true
     */
    public boolean sendPacketWithACK(Connection connection, byte[] packet, int timeout, boolean retry) {
        boolean isSuccess = false;
        boolean running = true;

        connection.send(packet);
        connection.setTimer(timeout);

        while (running && connection.isOpen()) {
            byte[] responseBytes = connection.receive();

            if (responseBytes != null) {
                Header.Content header = PacketHandler.getHeader(responseBytes);

                if (header != null) {
                    if (header.getType() == Constants.TYPE.ACK.getValue()) {
                        logger.info(String.format("[%s:%d] Received an acknowledgment for the request from the host.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                        running = false;
                        isSuccess = true;
                    } else if (header.getType() == Constants.TYPE.NACK.getValue()) {
                        logger.warn(String.format("[%s:%d] Received negative acknowledgment for the request from the host. Not retrying.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                        running = false;
                    } else {
                        logger.warn(String.format("[%s:%d] Received wrong packet type i.e. %d for the request from the host. Retrying.", connection.getDestinationIPAddress(), connection.getDestinationPort(), header.getType()));
                        connection.send(packet);
                    }
                } else {
                    logger.warn(String.format("[%s:%d] Received invalid header for the request from the host. Retrying.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                    connection.send(packet);
                }
            } else if (connection.isOpen()) {
                logger.warn(String.format("[%s:%d] Time-out happen for the packet to the host. Re-sending the packet.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                connection.send(packet);
            } else {
                logger.warn(String.format("[%s:%d] Connection is closed by the receiving end. Failed to send packet", connection.getDestinationIPAddress(), connection.getDestinationPort()));
            }

            running = retry && running;
        }

        connection.resetTimer();

        return isSuccess;
    }
}
