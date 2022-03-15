package controllers;

import configurations.BrokerConstants;
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

            if (header.getRequester() == BrokerConstants.REQUESTER.LOAD_BALANCER.getValue()) {

            } else if (header.getRequester() == BrokerConstants.REQUESTER.PRODUCER.getValue()) {

            } else if (header.getRequester() == BrokerConstants.REQUESTER.CONSUMER.getValue()) {

            }
        }
    }
}
