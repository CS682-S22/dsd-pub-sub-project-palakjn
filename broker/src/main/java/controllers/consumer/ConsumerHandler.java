package controllers.consumer;

import com.google.gson.reflect.TypeToken;
import configurations.BrokerConstants;
import controllers.Connection;
import controllers.DataTransfer;
import controllers.HostService;
import controllers.database.CacheManager;
import models.Header;
import models.Host;
import models.requests.TopicReadWriteRequest;
import models.requests.Request;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.BrokerPacketHandler;
import utilities.JSONDesrializer;

/**
 * Responsible for handling requests from Consumer.
 *
 * @author Palak Jain
 */
public class ConsumerHandler {
    private static final Logger logger = LogManager.getLogger(ConsumerHandler.class);
    private HostService hostService;
    private Connection connection;
    private Subscriber subscriber;
    private DataTransfer dataTransfer;

    public ConsumerHandler(Connection connection) {
        this.connection = connection;
        hostService = new HostService(logger);
        dataTransfer = new DataTransfer(connection);
    }

    /**
     * Identifies the action and process the request accordingly
     */
    public void processRequest(Header.Content header, byte[] message) {
        byte[] body = BrokerPacketHandler.getData(message);

        if (body != null) {
            if (header.getType() != BrokerConstants.TYPE.SUB.getValue() && header.getType() != BrokerConstants.TYPE.PULL.getValue()) {
                logger.warn(String.format("[%s:%d] Received invalid request %s from the consumer %s:%d.", connection.getSourceIPAddress(), connection.getSourcePort(), BrokerConstants.findTypeByValue(header.getType()), connection.getDestinationIPAddress(), connection.getDestinationPort()));
            } else {
                Request<TopicReadWriteRequest> request = JSONDesrializer.deserializeRequest(body, new TypeToken<Request<TopicReadWriteRequest>>(){}.getType());

                if (request != null && validateRequest(header, request.getRequest())) {
                    if (header.getType() == BrokerConstants.TYPE.PULL.getValue()) {
                        BrokerConstants.BROKER_STATE broker_state = CacheManager.getStatus(String.format("%s:%d", request.getRequest().getName(), request.getRequest().getPartition()));

                        if (broker_state == BrokerConstants.BROKER_STATE.READY) {
                            dataTransfer.setMethod(BrokerConstants.METHOD.PULL);
                            processPullRequest(request.getRequest());
                        } else {
                            logger.info(String.format("[%s] Broker is in %s state. Not sending any data to %s:%d. ", CacheManager.getBrokerInfo().getString(), broker_state.name(), connection.getDestinationIPAddress(), connection.getDestinationPort()));
                        }

                        connection.closeConnection();
                    } else {
                        //Adding subscriber
                        subscriber = new Subscriber(connection);
                        CacheManager.addSubscriber(subscriber);
                        dataTransfer.setMethod(BrokerConstants.METHOD.PUSH);
                        dataTransfer.setSubscriber(subscriber);
                        dataTransfer.processRequest(request.getRequest());
                    }
                }
            }
        }
    }

    /**
     * Validates whether the received request is valid, if broker holding partition information of the requested topic.
     */
    private boolean validateRequest(Header.Content header, TopicReadWriteRequest request) {
        boolean isValid = false;

        if (request != null && request.isValid()) {
            logger.debug(String.format("[%s:%d] Received [%s] request to get the data of topic %s - partition %d - offset - %d from the consumer %s:%d", connection.getSourceIPAddress(), connection.getSourcePort(), BrokerConstants.findTypeByValue(header.getType()), request.getName(), request.getPartition(), request.getFromOffset(), connection.getDestinationIPAddress(), connection.getDestinationPort()));

            if (CacheManager.isExist(request.getName(), request.getPartition())) {
                logger.warn(String.format("[%s:%d] [%s] Broker holding the topic %s - partition %d information.", connection.getSourceIPAddress(), connection.getSourcePort(), BrokerConstants.findTypeByValue(header.getType()), request.getName(), request.getPartition()));
                hostService.sendACK(connection, BrokerConstants.REQUESTER.BROKER, header.getSeqNum());

                isValid = true;
            } else {
                logger.warn(String.format("[%s:%d] Broker not holding the topic %s - partition %d information.", connection.getDestinationIPAddress(), connection.getDestinationPort(), request.getName(), request.getPartition()));
                hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER);
            }
        } else {
            logger.warn(String.format("[%s:%d] Received invalid request body from consumer %s:%d.", connection.getSourceIPAddress(), connection.getSourcePort(), connection.getDestinationIPAddress(), connection.getDestinationPort()));
            hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER);
        }

        return isValid;
    }

    /**
     * Process pull request where it sends the requested number of logs and again read for new PULL request
     */
    private void processPullRequest(TopicReadWriteRequest request) {
        while (connection.isOpen()) {
            dataTransfer.processRequest(request);

            request = receivePullRequest();
        }
    }

    /**
     * Receive pull request from consumer, return request if it is a valid request else null
     */
    private TopicReadWriteRequest receivePullRequest() {
        TopicReadWriteRequest readTopicRequest = null;

        while (readTopicRequest == null) {
            byte[] packet = connection.receive();

            if (packet != null) {
                Header.Content header = BrokerPacketHandler.getHeader(packet);

                if (header != null && header.getRequester() == BrokerConstants.REQUESTER.CONSUMER.getValue() && header.getType() == BrokerConstants.TYPE.PULL.getValue()) {
                    byte[] body = BrokerPacketHandler.getData(packet);

                    if (body != null) {
                        Request<TopicReadWriteRequest> request = JSONDesrializer.deserializeRequest(body, new TypeToken<Request<TopicReadWriteRequest>>(){}.getType());

                        if (request != null && validateRequest(header, request.getRequest())) {
                            readTopicRequest = request.getRequest();
                        }
                    }
                }
            }
        }

        return readTopicRequest;
    }


}
