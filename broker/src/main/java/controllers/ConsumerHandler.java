package controllers;

import configuration.Constants;
import configurations.BrokerConstants;
import models.File;
import models.Header;
import models.Request;
import models.Segment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.BrokerPacketHandler;
import utilities.JSONDesrializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class ConsumerHandler {
    private static final Logger logger = LogManager.getLogger(ConsumerHandler.class);
    private HostService hostService;
    private Connection connection;

    public ConsumerHandler(Connection connection) {
        this.connection = connection;
        hostService = new HostService(logger);
    }

    public void processRequest(Header.Content header, byte[] message) {
        byte[] body = BrokerPacketHandler.getData(message);

        if (body != null) {
            if (header.getType() == BrokerConstants.TYPE.ADD.getValue()) {
                //Get the consumer detail and add it as the subscribers
            } else if (header.getType() == BrokerConstants.TYPE.PULL.getValue()) {
                logger.info(String.format("[%s:%d] Received pull request from the consumer.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                hostService.sendACK(connection, BrokerConstants.REQUESTER.CONSUMER, header.getSeqNum());
                //Pull request
                processPullRequest(body);
            }
        }
    }

    private void processPullRequest(byte[] body) {
        Request request = JSONDesrializer.fromJson(body, Request.class);

        if (request != null && request.isValid()) {
            logger.debug(String.format("[%s:%d] Received request to get the data of topic %s - partition %d - offset - %d", connection.getDestinationIPAddress(), connection.getDestinationPort(), request.getTopicName(), request.getPartition(), request.getOffset()));
            if (CacheManager.isExist(request.getTopicName(), request.getPartition())) {
                File partition = CacheManager.getPartition(request.getTopicName(), request.getPartition());

                //Checking if we have the message with the given offset
                int segmentNumber = partition.getSegmentNumber(request.getOffset());
                if (segmentNumber != -1) {
                    logger.debug(String.format("[%s:%d] Segment %d holding information of %d offset", connection.getDestinationIPAddress(), connection.getDestinationPort(), segmentNumber, request.getOffset()));
                    send(partition, segmentNumber, request.getOffset());
                } else {
                    logger.warn(String.format("[%s:%d] No offset %d found for the topic %s - partition %d information.", connection.getDestinationIPAddress(), connection.getDestinationPort(), request.getOffset(), request.getTopicName(), request.getPartition()));
                    hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER);
                }
            } else {
                logger.warn(String.format("[%s:%d] Broker not holding topic %s - partition %d information.", connection.getDestinationIPAddress(), connection.getDestinationPort(), request.getTopicName(), request.getPartition()));
                hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER);
            }
        } else {
            logger.warn(String.format("[%s:%d] Invalid request body.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
            hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER);
        }
    }

    private void send(File partition, int segmentNumber, int offset) {
        List<Segment> segments = partition.getSegmentsFrom(segmentNumber);

        for (Segment segment : segments) {
            try (FileInputStream stream = new FileInputStream(segment.getLocation())) {
                int index = 0;

                if (segment.getSegment() == segmentNumber) {
                    //Getting the index of the offset which contains the starting offset
                    index = segment.getOffsetIndex(offset);
                }

                while (index < segment.getNumOfOffsets()) {
                    int length = 0;
                    if (index + 1 < segment.getNumOfOffsets()) {
                        length = segment.getOffset(index + 1) - segment.getOffset(index);
                    } else {
                        length = segment.getAvailableSize() - segment.getOffset(index);
                    }

                    byte[] data = new byte[length];
                    int result = stream.read(data, 0, length);
                    if(result == length) {
                        connection.send(BrokerPacketHandler.createDataPacket(data));
                        logger.debug(String.format("Send %d number of bytes to the consumer.", result));
                    } else {
                        logger.warn(String.format("Not able to send data. Read %d number of bytes. Expected %d number of bytes.", result, length));
                    }

                    index++;
                }
            } catch (IndexOutOfBoundsException | IOException ioException) {
                logger.error(String.format("Unable to open the segment file at the location %s.", segment.getLocation()), ioException);
            }
        }
    }
}
