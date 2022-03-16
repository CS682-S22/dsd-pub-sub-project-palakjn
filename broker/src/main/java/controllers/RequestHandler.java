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
        boolean running = true;
        int curSeq = 0;

        while (running) {
            byte[] request = connection.receive();

            if (request != null) {
                Header.Content header = BrokerPacketHandler.getHeader(request);

                if (header.getRequester() == BrokerConstants.REQUESTER.LOAD_BALANCER.getValue()) {
                    logger.info("Received request from Load Balancer.");
                    if (header.getSeqNum() == curSeq) {
                        LBHandler lbHandler = new LBHandler(connection);
                        if (lbHandler.processRequest(header, request)) {
                            curSeq = curSeq == 0 ? 1 : 0;
                        }
                    } else if ((curSeq == 0 && header.getSeqNum() == 1) || (curSeq == 1 && header.getSeqNum() == 0)) {
                        hostService.sendACK(BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
                    }
                } else if (header.getRequester() == BrokerConstants.REQUESTER.PRODUCER.getValue()) {
                    logger.info("Received request from Producer.");
                    ProducerHandler producerHandler = new ProducerHandler(connection);
                    producerHandler.processRequest(header, request);
                    running = false;
                } else if (header.getRequester() == BrokerConstants.REQUESTER.CONSUMER.getValue()) {
                    logger.info("Received request from Consumer.");
                }
            } else {
                running = false;
            }
        }
    }
}
