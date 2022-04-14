package controllers;

import configurations.BrokerConstants;
import models.*;
import models.requests.Request;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.BrokerPacketHandler;
import utilities.JSONDesrializer;

import java.io.IOException;
import java.net.Socket;

/**
 * Responsible for handling requests from load balancer.
 *
 * @author Palak Jain
 */
public class LBHandler {
    private static final Logger logger = LogManager.getLogger(LBHandler.class);
    private HostService hostService;
    private Connection connection;

    public LBHandler(Connection connection) {
        this.connection = connection;
        hostService = new HostService(logger);
    }

    public LBHandler() {
        hostService = new HostService(logger);
    }

    /**
     * Send the request to load balancer to join the network
     */
    public boolean join(Host brokerInfo, Host loadBalancerInfo) {
        return send(brokerInfo, loadBalancerInfo, BrokerConstants.REQUEST_TYPE.ADD);
    }

    /**
     * Send the request to load balancer to remove it from the network
     */
    public boolean remove(Host brokerInfo, Host loadBalancerInfo) {
        return send(brokerInfo, loadBalancerInfo, BrokerConstants.REQUEST_TYPE.REM);
    }

    /**
     * Process the request based on the action received from the load balancer
     */
    public boolean processRequest(Header.Content header, byte[] packet) {
        boolean isSuccess = false;

        if (header.getType() == BrokerConstants.TYPE.REQ.getValue()) {
            //Adding new topic information
            byte[] body = BrokerPacketHandler.getData(packet);

            if (body != null) {
                Request<Topic> topicRequest = JSONDesrializer.fromJson(body, Request.class);
                Topic topic = null;

                if (topicRequest != null) {
                    topic = topicRequest.getRequest();
                }

                if (topic != null && topic.isValid()) {
                    if (topicRequest.getType().equalsIgnoreCase(BrokerConstants.REQUEST_TYPE.ADD)) {
                        logger.info(String.format("Received request to handle %d number of partitions for a topic %s.", topic.getNumOfPartitions(), topic.getName()));

                        //First checking if topic already exits
                        if (CacheManager.iSTopicExist(topic.getName())) {
                            logger.warn(String.format("[%s:%d] Broker already handling the topic %s. Will send NACK.", connection.getDestinationIPAddress(), connection.getDestinationPort(), topic.getName()));
                            hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
                        } else if (topic.getNumOfPartitions() > 0) {
                            CacheManager.addTopic(topic.getName());

                            for (Partition partition : topic.getPartitions()) {
                                File file = new File(partition.getTopicName(), partition.getNumber());
                                if (file.initialize(String.format(BrokerConstants.TOPIC_LOCATION, connection.getSourceIPAddress(), connection.getSourcePort()), partition.getTopicName(), partition.getNumber())) {
                                    CacheManager.addPartition(partition.getTopicName(), partition.getNumber(), file);

                                    //Adding followers if the current broker is leader
                                    if (isLeader(partition.getLeader())) {
                                        for (Host follower : partition.getBrokers()) {
                                            if (!follower.isLeader()) {
                                                CacheManager.addFollower(file.getName(), follower);
                                            }
                                        }
                                    }

                                    logger.info(String.format("[%s:%d] Added topic %s - partition %d information to the local cache.", connection.getDestinationIPAddress(), connection.getDestinationPort(), partition.getTopicName(), partition.getNumber()));
                                    System.out.printf("Handling topic %s - partition %d.\n", partition.getTopicName(), partition.getNumber());
                                } else {
                                    logger.warn(String.format("[%s:%d] Fail to create directory for the topic- %s - Partition %d", connection.getDestinationIPAddress(), connection.getDestinationPort(), partition.getTopicName(), partition.getNumber()));
                                }
                            }

                            hostService.sendACK(connection, BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
                            isSuccess = true;
                        } else {
                            logger.warn(String.format("[%s:%d] Received no partitions for the topic %s from the load balancer. Will send NACK", connection.getDestinationIPAddress(), connection.getDestinationPort(), topic.getName()));
                            hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
                        }
                    }
                } else {
                    logger.warn(String.format("[%s:%d] Received invalid topic information from the load balancer. Will send NACK", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                    hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
                }
            } else {
                logger.warn(String.format("[%s:%d] Received empty request body from the producer. Sending NACK.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
            }
        } else {
            logger.warn(String.format("[%s:%d] Received unsupported action %d from the load balancer. Will send NACK", connection.getDestinationIPAddress(), connection.getDestinationPort(), header.getType()));
            hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
        }

        return isSuccess;
    }

    /**
     * Send the request to add/remove to the load balancer
     */
    private boolean send(Host brokerInfo, Host loadBalancerInfo, String type) {
        boolean isSuccess = false;

        try {
            Socket socket = new Socket(loadBalancerInfo.getAddress(), loadBalancerInfo.getPort());
            logger.info(String.format("[%s:%d] Successfully connected to load balancer %s: %d.", brokerInfo.getAddress(), brokerInfo.getPort(), loadBalancerInfo.getAddress(), loadBalancerInfo.getPort()));

            Connection connection = new Connection(socket, loadBalancerInfo.getAddress(), loadBalancerInfo.getPort(), brokerInfo.getAddress(), brokerInfo.getPort());
            if (connection.openConnection()) {
                byte[] packet = BrokerPacketHandler.createPacket(brokerInfo, type);
                isSuccess = hostService.sendPacketWithACK(connection, packet, String.format("%s:%s", BrokerConstants.REQUESTER.BROKER.name(), type));
                if (isSuccess) {
                    logger.info("Successfully joined to the network");
                    System.out.println("Successfully joined to the network");
                }
            }
        } catch (IOException exception) {
            logger.error(String.format("[%s:%d] Fail to make connection with the load balancer %s:%d. ", brokerInfo.getAddress(), brokerInfo.getPort(), loadBalancerInfo.getAddress(), loadBalancerInfo.getPort()), exception);
        }

        return isSuccess;
    }

    /**
     * Checks if the current broker is leader
     */
    private boolean isLeader(Host leader) {
        return leader.getAddress().equals(connection.getSourceIPAddress()) && leader.getPort() == connection.getSourcePort();
    }
}
