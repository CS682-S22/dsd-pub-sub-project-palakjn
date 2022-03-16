package controllers;

import configuration.Constants;
import configurations.BrokerConstants;
import models.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import utilities.BrokerPacketHandler;
import utilities.JSONDesrializer;

import java.io.IOException;
import java.net.Socket;

public class LBHandler {
    private static final Logger logger = LogManager.getLogger(LBHandler.class);
    private HostService hostService;
    private Connection connection;

    public LBHandler(Connection connection) {
        hostService = new HostService(connection, logger);
    }

    public LBHandler() {}

    public boolean join(Host brokerInfo, Host loadBalancerInfo) {
        return send(brokerInfo, loadBalancerInfo, BrokerConstants.TYPE.ADD);
    }

    public boolean remove(Host brokerInfo, Host loadBalancerInfo) {
        return send(brokerInfo, loadBalancerInfo, BrokerConstants.TYPE.REM);
    }

    public boolean processRequest(Header.Content header, byte[] request) {
        boolean isSuccess = false;

        if (header.getType() == BrokerConstants.TYPE.ADD.getValue()) {
            //Adding new topic information
            byte[] body = BrokerPacketHandler.getData(request);

            if (body != null) {
                Topic topic = JSONDesrializer.fromJson(body, Topic.class);

                if (topic != null && topic.isValid()) {
                    logger.info(String.format("Received request to handle %d number of partitions for a topic %s.", topic.getNumOfPartitions(), topic.getName()));

                    //First checking if topic already exits
                    if (CacheManager.iSTopicExist(topic.getName())) {
                        logger.warn(String.format("[%s:%d] Broker already handling the topic %s. Will send NACK.", connection.getDestinationIPAddress(), connection.getDestinationPort(), topic.getName()));
                        hostService.sendNACK(BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
                    } else if (topic.getNumOfPartitions() > 0) {
                        CacheManager.addTopic(topic.getName());

                        for (Partition partition : topic.getPartitions()) {
                            File file = new File();
                            file.initialize(String.format(BrokerConstants.TOPIC_LOCATION, connection.getSourceIPAddress(), connection.getSourcePort()), partition.getTopicName(), partition.getNumber());
                            CacheManager.addPartition(partition.getTopicName(), partition.getNumber(), file);
                            logger.info(String.format("[%s:%d] Added topic %s - partition %d information to the local cache.", connection.getDestinationIPAddress(), connection.getDestinationPort(), partition.getTopicName(), partition.getNumber()));
                        }

                        hostService.sendACK(BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
                        isSuccess = true;
                    } else {
                        logger.warn(String.format("[%s:%d] Received no partitions for the topic %s from the load balancer. Will send NACK", connection.getDestinationIPAddress(), connection.getDestinationPort(), topic.getName()));
                        hostService.sendNACK(BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
                    }
                } else {
                    logger.warn(String.format("[%s:%d] Received invalid topic information from the load balancer. Will send NACK", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                    hostService.sendNACK(BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
                }
            } else {
                logger.warn(String.format("[%s:%d] Received empty request body from the producer. Sending NACK.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                hostService.sendNACK(Constants.REQUESTER.BROKER, header.getSeqNum());
            }
        } else {
            logger.warn(String.format("[%s:%d] Received unsupported action %d from the load balancer. Will send NACK", connection.getDestinationIPAddress(), connection.getDestinationPort(), header.getType()));
            hostService.sendNACK(BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
        }

        return isSuccess;
    }

    private boolean send(Host brokerInfo, Host loadBalancerInfo, BrokerConstants.TYPE type) {
        boolean isSuccess = false;

        try {
            Socket socket = new Socket(loadBalancerInfo.getAddress(), loadBalancerInfo.getPort());
            logger.info(String.format("[%s:%d] Successfully connected to load balancer %s: %d.", brokerInfo.getAddress(), brokerInfo.getPort(), loadBalancerInfo.getAddress(), loadBalancerInfo.getPort()));

            Connection connection = new Connection(socket, loadBalancerInfo.getAddress(), loadBalancerInfo.getPort(), brokerInfo.getAddress(), brokerInfo.getPort());
            if (connection.openConnection()) {
                HostService service = new HostService(connection, logger);
                byte[] packet = BrokerPacketHandler.createPacket(brokerInfo, type);
                isSuccess = service.sendPacketWithACK(packet, String.format("%s:%s", BrokerConstants.REQUESTER.BROKER.name(), type.name()));
            }
        } catch (IOException exception) {
            logger.error(String.format("[%s:%d] Fail to make connection with the load balancer %s:%d. ", brokerInfo.getAddress(), brokerInfo.getPort(), loadBalancerInfo.getAddress(), loadBalancerInfo.getPort()), exception);
        }

        return isSuccess;
    }
}
