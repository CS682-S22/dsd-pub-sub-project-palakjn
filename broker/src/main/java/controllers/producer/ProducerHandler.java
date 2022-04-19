package controllers.producer;

import configuration.Constants;
import configurations.BrokerConstants;
import controllers.Connection;
import controllers.HostService;
import controllers.consumer.Subscriber;
import controllers.database.CacheManager;
import models.Header;
import models.data.File;
import models.requests.Request;
import models.requests.TopicReadWriteRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.BrokerPacketHandler;
import utilities.JSONDesrializer;

/**
 * Responsible for handling requests from producer.
 *
 * @author Palak Jain
 */
public class ProducerHandler {
    private static final Logger logger = LogManager.getLogger(ProducerHandler.class);
    private HostService hostService;
    private Connection connection;

    public ProducerHandler(Connection connection) {
        this.connection = connection;
        hostService = new HostService(logger);
    }

    /**
     * Process the request from producer based on the action.
     */
    public void processRequest(Header.Content header, byte[] message) {
        if (header.getType() == BrokerConstants.TYPE.REQ.getValue()) {
            byte[] body = BrokerPacketHandler.getData(message);

            if (body != null) {
                Request<TopicReadWriteRequest> request = JSONDesrializer.deserializeRequest(body, TopicReadWriteRequest.class);
                TopicReadWriteRequest writeTopicRequest = null;

                if (request != null) {
                    writeTopicRequest = request.getRequest();
                }

                if (writeTopicRequest != null && writeTopicRequest.isValid()) {
                    logger.info(String.format("[%s:%d] Received request to add the logs for the topic %s - partition %d from the producer %s:%d.", connection.getSourceIPAddress(), connection.getSourcePort(), writeTopicRequest.getName(), writeTopicRequest.getPartition(), connection.getDestinationIPAddress(), connection.getDestinationPort()));

                    //Checks if broker handling the topic-partition information or not
                    if (CacheManager.isExist(writeTopicRequest.getName(), writeTopicRequest.getPartition())) {
                        hostService.sendACK(connection, BrokerConstants.REQUESTER.BROKER, header.getSeqNum());

                        File file = CacheManager.getPartition(writeTopicRequest.getName(), writeTopicRequest.getPartition());
                        receive(file);
                    } else {
                        logger.warn(String.format("[%s:%d] Current broker %s:%d not holding any topic %s - partition %d. Sending NACK.", connection.getDestinationIPAddress(), connection.getDestinationPort(), connection.getSourceIPAddress(), connection.getSourcePort(), writeTopicRequest.getName(), writeTopicRequest.getPartition()));
                        hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
                    }
                } else {
                    logger.warn(String.format("[%s:%d] Received invalid request information from the producer %s:%d. Sending NACK", connection.getSourceIPAddress(), connection.getSourcePort(), connection.getDestinationIPAddress(), connection.getDestinationPort()));
                    hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
                }
            } else {
                logger.warn(String.format("[%s:%d] Received empty request body from the producer %s:%d. Sending NACK.", connection.getSourceIPAddress(), connection.getSourcePort(), connection.getDestinationIPAddress(), connection.getDestinationPort()));
                hostService.sendNACK(connection, Constants.REQUESTER.BROKER, header.getSeqNum());
            }
        } else {
            logger.warn(String.format("[%s:%d] Received unsupported action %d from the producer %s:%d. Sending NACK", connection.getSourceIPAddress(), connection.getSourcePort(), header.getType(), connection.getDestinationIPAddress(), connection.getDestinationPort()));
            hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
        }
    }

    /**
     * Receives data from publisher, write to the local segment as well as send to all the subscribers.
     */
    private void receive(File partition) {
        boolean reading = true;
        //Sequence number of the expecting packet
        int seqNum = 0;

        while (reading && connection.isOpen()) {
            byte[] message = connection.receive();

            if (message != null) {
                Header.Content header = BrokerPacketHandler.getHeader(message);

                if (header != null) {
                    if (header.getType() == BrokerConstants.TYPE.DATA.getValue()) {
                        if (header.getSeqNum() == seqNum) {
                            BrokerConstants.BROKER_STATE broker_state = CacheManager.getStatus(partition.getName());

                            if (broker_state == BrokerConstants.BROKER_STATE.READY) {
                                byte[] data = BrokerPacketHandler.getData(message);

                                if (data != null) {
                                    //Sending to all the subscribers.
                                    sendToSubscribers(data);

                                    //Sending to all the followers if the current broker is the leader
                                    if (partition.write(data, CacheManager.isLeader(partition.getName(), CacheManager.getBrokerInfo()))) {
                                        //Sending ACK to producer only once replicate data to the followers
                                        hostService.sendACK(connection, BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
                                        seqNum++;
                                    }
                                    logger.info(String.format("[%s:%d] Received data %d from producer %s:%d. Written to the segment.", connection.getSourceIPAddress(), connection.getSourcePort(), seqNum, connection.getDestinationIPAddress(), connection.getDestinationPort()));
                                } else {
                                    logger.warn(String.format("[%s:%d] Received empty data %d from producer %s:%d.", connection.getSourceIPAddress(), connection.getSourcePort(), seqNum, connection.getDestinationIPAddress(), connection.getDestinationPort()));
                                }
                            } else {
                                logger.info(String.format("[%s:%d] Broker is in %s state. Not accepting new data from producer %s:%d. Sending NACK.", connection.getSourceIPAddress(), connection.getSourcePort(), broker_state.name(), connection.getDestinationIPAddress(), connection.getDestinationPort()));
                                hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
                            }
                        } else if (header.getSeqNum() < seqNum) {
                            logger.warn(String.format("[%s:%d] Received data from producer %s:%d with the seqNum as %d. Expecting %d. Sending ACK.", connection.getSourceIPAddress(), connection.getSourcePort(), connection.getDestinationIPAddress(), connection.getDestinationPort(), header.getSeqNum(), seqNum));
                            hostService.sendACK(connection, BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
                        } else {
                            logger.warn(String.format("[%s:%d] Received data from producer %s:%d with the seqNum as %d. Expecting %d. Ignoring the data.", connection.getSourceIPAddress(), connection.getSourcePort(), connection.getDestinationIPAddress(), connection.getDestinationPort(), header.getSeqNum(), seqNum));
                        }
                    } else if (header.getType() == BrokerConstants.TYPE.REQ.getValue()) {
                        logger.info(String.format("[%s:%d] Received REQ request from producer %s:%d again. Sending ACK as ACK might have lost before.", connection.getSourceIPAddress(), connection.getSourcePort(), connection.getDestinationIPAddress(), connection.getDestinationPort()));
                        hostService.sendACK(connection, BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
                    }
                } else {
                    logger.warn(String.format("[%s:%d] Received invalid packet from the producer %s:%d.", connection.getSourceIPAddress(), connection.getSourcePort(), connection.getDestinationIPAddress(), connection.getDestinationPort()));
                }
            } else {
                logger.warn(String.format("[%s:%d] Channel might have closed by producer %s:%d. Not reading further from the channel.", connection.getSourceIPAddress(), connection.getSourcePort(), connection.getDestinationIPAddress(), connection.getDestinationPort()));
                reading = false;
            }
        }
    }

    /**
     * Send the data to all the subscribers which broker handling.
     */
    private void sendToSubscribers(byte[] data) {
        int numOfSubscribers = CacheManager.getSubscribersCount();

        for (int index = 0; index < numOfSubscribers; index++) {
            Subscriber subscriber = CacheManager.getSubscriber(index);

            subscriber.onEvent(data);
            logger.info(String.format("[%s:%d] Send data to the subscriber: %s.", connection.getSourceIPAddress(), connection.getSourcePort(), subscriber.getAddress()));
        }
    }
}
