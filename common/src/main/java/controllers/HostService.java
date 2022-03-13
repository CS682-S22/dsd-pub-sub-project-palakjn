package controllers;

import configuration.Constants;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import utilities.PacketHandler;

public class HostService {
    private static final Logger logger = LogManager.getLogger(HostService.class);
    private Connection connection;

    public HostService(Connection connection) {
        this.connection = connection;
    }

    public void sendACK(Constants.REQUESTER requester) {
        byte[] acknowledgement = PacketHandler.createHeader(requester, Constants.TYPE.ACK);
        connection.send(acknowledgement);
    }

    public void sendNACK(Constants.REQUESTER requester) {
        byte[] negAck = PacketHandler.createHeader(requester, Constants.TYPE.NACK);
        connection.send(negAck);
    }
}
