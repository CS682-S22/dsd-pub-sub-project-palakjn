package controllers;

import configuration.Constants;
import configurations.BrokerConstants;
import models.File;
import models.Header;
import models.Request;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.BrokerPacketHandler;
import utilities.JSONDesrializer;

public class ProducerHandler {
    private static final Logger logger = LogManager.getLogger(ProducerHandler.class);
    private HostService hostService;
    private Connection connection;

    public ProducerHandler(Connection connection) {
        this.connection = connection;
        hostService = new HostService(logger);
    }

    public void processRequest(Header.Content header, byte[] message) {
        if (header.getType() == BrokerConstants.TYPE.ADD.getValue()) {
            byte[] body = BrokerPacketHandler.getData(message);

            if (body != null) {
                Request request = JSONDesrializer.fromJson(body, Request.class);

                if (request != null && request.isValid()) {
                    logger.info(String.format("[%s:%d] Received request to add the logs for the topic %s - partition %d from the producer.", connection.getDestinationIPAddress(), connection.getDestinationPort(), request.getTopicName(), request.getPartition()));

                    if (CacheManager.isExist(request.getTopicName(), request.getPartition())) {
                        hostService.sendACK(connection, BrokerConstants.REQUESTER.BROKER, header.getSeqNum());

                        File file = CacheManager.getPartition(request.getTopicName(), request.getPartition());
                        receive(file);
                    } else {
                        logger.warn(String.format("[%s:%d] Current broker not holding any topic %s - partition %d. Sending NACK.", connection.getDestinationIPAddress(), connection.getDestinationPort(), request.getTopicName(), request.getPartition()));
                        hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
                    }
                } else {
                    logger.warn(String.format("[%s:%d] Received invalid request information from the producer. Sending NACK", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                    hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
                }
            } else {
                logger.warn(String.format("[%s:%d] Received empty request body from the producer. Sending NACK.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                hostService.sendNACK(connection, Constants.REQUESTER.BROKER, header.getSeqNum());
            }
        } else {
            logger.warn(String.format("[%s:%d] Received unsupported action %d from the producer. Sending NACK", connection.getDestinationIPAddress(), connection.getDestinationPort(), header.getType()));
            hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
        }
    }

    private void receive(File partition) {
        boolean reading = true;

        while (reading) {
            logger.debug("Waiting for new data");
            byte[] message = connection.receive();

            if (message != null) {
                Header.Content header = BrokerPacketHandler.getHeader(message);

                if (header != null) {
                    if (header.getType() == BrokerConstants.TYPE.DATA.getValue()) {
                        byte[] data = BrokerPacketHandler.getData(message);

                        if (data != null) {
                            logger.info(String.format("[%s:%d] Received data from producer. Writing to the segment.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                            partition.write(data);
                        } else {
                            logger.warn(String.format("[%s:%d] Received empty data from producer.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                        }
                    } else if (header.getType() == BrokerConstants.TYPE.ADD.getValue()) {
                        logger.info(String.format("[%s:%d] Received ADD request from producer again. Sending ACK as ACK might have lost before.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                        hostService.sendACK(connection, BrokerConstants.REQUESTER.BROKER, header.getSeqNum());
                    } else if (header.getType() == BrokerConstants.TYPE.FIN.getValue()) {
                        logger.info(String.format("[%s:%d] Received FIN from producer. Not reading anymore from the channel.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                        reading = false;
                    }
                } else {
                    logger.warn(String.format("[%s:%d] Received invalid packet from the producer.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                }
            } else {
                logger.warn(String.format("[%s:%d] Channel might have closed by producer. Not reading further from the channel.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                reading = false;
            }
        }
    }
}
