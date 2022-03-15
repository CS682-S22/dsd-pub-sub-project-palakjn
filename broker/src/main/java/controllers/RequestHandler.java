package controllers;

import configuration.Constants;
import models.Header;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import utilities.BrokerPacketHandler;

public class RequestHandler {

    private static final Logger logger = LogManager.getLogger(RequestHandler.class);
    private HostService hostService;
    private Connection connection;
    private int curSeq = 0;

    public RequestHandler(Connection connection) {
        this.connection = connection;
        this.hostService = new HostService(connection, logger);
    }

    public void process() {
        byte[] request = connection.receive();

        if (request != null) {
            Header.Content header = BrokerPacketHandler.getHeader(request);

            if (header.getRequester() == Constants.REQUESTER.LOAD_BALANCER.getValue()) {

            } else if (header.getRequester() == Constants.REQUESTER.PRODUCER.getValue()) {

            } else if (header.getRequester() == Constants.REQUESTER.CONSUMER.getValue()) {

            }
        }
    }
}
