package controllers;

import configurations.BrokerConstants;
import controllers.broker.BrokerHandler;
import controllers.consumer.ConsumerHandler;
import controllers.producer.ProducerHandler;
import models.Header;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.BrokerPacketHandler;

/**
 * Responsible for handling requests from other hosts.
 *
 * @author Palak Jain
 */
public class RequestHandler {
    private static final Logger logger = LogManager.getLogger(RequestHandler.class);
    private HostService hostService;
    private Connection connection;

    public RequestHandler(Connection connection) {
        this.connection = connection;
        this.hostService = new HostService(logger);
    }

    /**
     * Calls appropriate handler to process the request based on who made the request.
     */
    public void process() {
        boolean running = true;

        while (running && connection.isOpen()) {
            byte[] request = connection.receive();

            if (request != null) {
                Header.Content header = BrokerPacketHandler.getHeader(request);

                if (header.getRequester() == BrokerConstants.REQUESTER.PRODUCER.getValue()) {
                    logger.info("Received request from Producer.");
                    ProducerHandler producerHandler = new ProducerHandler(connection);
                    producerHandler.processRequest(header, request);
                    running = false;
                } else if (header.getRequester() == BrokerConstants.REQUESTER.CONSUMER.getValue()) {
                    logger.info("Received request from Consumer.");
                    ConsumerHandler consumerHandler = new ConsumerHandler(connection);
                    consumerHandler.processRequest(header, request);
                    running = false;
                } else if (header.getRequester() == BrokerConstants.REQUESTER.BROKER.getValue()) {
                    logger.info("Received request from Broker");
                    BrokerHandler brokerHandler = new BrokerHandler(connection);
                    brokerHandler.handleRequest(header, request);
                }
            } else {
                running = false;
            }
        }
    }
}
