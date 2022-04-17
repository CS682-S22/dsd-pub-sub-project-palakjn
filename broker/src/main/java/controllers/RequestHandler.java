package controllers;

import configurations.BrokerConstants;
import controllers.consumer.ConsumerHandler;
import controllers.database.CacheManager;
import controllers.election.Election;
import controllers.heartbeat.HeartBeatReceiver;
import controllers.producer.ProducerHandler;
import controllers.replication.Broker;
import models.Header;
import models.requests.BrokerUpdateRequest;
import models.requests.Request;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.BrokerPacketHandler;
import utilities.JSONDesrializer;

import java.util.Objects;

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
                    if (header.getType() == BrokerConstants.TYPE.HEARTBEAT.getValue()) {
                        //TODO: Log
                        HeartBeatReceiver heartBeatReceiver = new HeartBeatReceiver();
                        heartBeatReceiver.handleRequest(connection, request);
                    } else if (header.getType() == BrokerConstants.TYPE.ELECTION.getValue()) {
                        //TODO: Log
                        hostService.sendACK(connection, BrokerConstants.REQUESTER.BROKER);
                        Election election = new Election();
                        election.handleRequest(request);
                    } else if (header.getType() == BrokerConstants.TYPE.UPDATE.getValue()) {
                        //TODO: Log
                        byte[] data = BrokerPacketHandler.getData(request);

                        if (data != null) {
                            Request<BrokerUpdateRequest> brokerUpdateRequest = JSONDesrializer.fromJson(data, Request.class);

                            if (brokerUpdateRequest != null && brokerUpdateRequest.getRequest() != null) {
                                if (Objects.equals(brokerUpdateRequest.getType(), BrokerConstants.REQUEST_TYPE.LEADER)) {
                                    //TODO: Log
                                    CacheManager.setLeader(brokerUpdateRequest.getRequest().getTopic(), new Broker(brokerUpdateRequest.getRequest().getBroker()));
                                    hostService.sendACK(connection, BrokerConstants.REQUESTER.BROKER);
                                    CacheManager.setStatus(brokerUpdateRequest.getRequest().getTopic(), BrokerConstants.BROKER_STATE.SYNC);
                                    //TODO: Call sync module
                                } else {
                                    //TODO: log
                                }
                            }
                        }

                    }
                }
            } else {
                running = false;
            }
        }
    }
}
