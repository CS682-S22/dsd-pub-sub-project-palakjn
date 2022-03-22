package controllers;

import configuration.Constants;
import configurations.BrokerConstants;
import models.*;
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
    private Subscriber subscriber;
    private BrokerConstants.METHOD method;

    public ConsumerHandler(Connection connection) {
        this.connection = connection;
        hostService = new HostService(logger);
    }

    public void processRequest(Header.Content header, byte[] message) {
        byte[] body = BrokerPacketHandler.getData(message);

        if (body != null) {
            if (header.getType() != BrokerConstants.TYPE.ADD.getValue() && header.getType() != BrokerConstants.TYPE.PULL.getValue()) {
                logger.warn(String.format("[%s:%d] Received invalid request %s from the consumer %s:%d.", connection.getSourceIPAddress(), connection.getSourcePort(), BrokerConstants.findTypeByValue(header.getType()), connection.getDestinationIPAddress(), connection.getDestinationPort()));
            } else {
                Request request = JSONDesrializer.fromJson(body, Request.class);

                if (request != null && request.isValid()) {
                    logger.debug(String.format("[%s:%d] Received [%s] request to get the data of topic %s - partition %d - offset - %d from the consumer %s:%d", connection.getSourceIPAddress(), connection.getSourcePort(), BrokerConstants.findTypeByValue(header.getType()), request.getTopicName(), request.getPartition(), request.getOffset(), connection.getDestinationIPAddress(), connection.getDestinationPort()));

                    if (CacheManager.isExist(request.getTopicName(), request.getPartition())) {
                        logger.warn(String.format("[%s:%d] [%s] Broker holding the topic %s - partition %d information.", connection.getSourceIPAddress(), connection.getSourcePort(), BrokerConstants.findTypeByValue(header.getType()), request.getTopicName(), request.getPartition()));
                        hostService.sendACK(connection, BrokerConstants.REQUESTER.CONSUMER, header.getSeqNum());

                        if (header.getType() == BrokerConstants.TYPE.ADD.getValue()) {

                            //Adding subscriber
                            subscriber = new Subscriber(connection);
                            CacheManager.addSubscriber(subscriber);
                        }

                        setMethod(header);

                        processRequest(request);

                        if (header.getType() == BrokerConstants.TYPE.PULL.getValue()) {
                            //Closing the connection after sending data
                            connection.closeConnection();
                        }
                    } else {
                        logger.warn(String.format("[%s:%d] Broker not holding the topic %s - partition %d information.", connection.getDestinationIPAddress(), connection.getDestinationPort(), request.getTopicName(), request.getPartition()));
                        hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER);
                    }
                } else {
                    logger.warn(String.format("[%s:%d] Received invalid request body from consumer %s:%d.", connection.getSourceIPAddress(), connection.getSourcePort(), connection.getDestinationIPAddress(), connection.getDestinationPort()));
                    hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER);
                }
            }
        }
    }

    private void processRequest(Request request) {
        File partition = CacheManager.getPartition(request.getTopicName(), request.getPartition());

        int segmentNumber = getSegmentNumber(request, partition);

        if (segmentNumber != -1) {
            logger.debug(String.format("[%s:%d] [%s] Segment %d holding information of %d offset", connection.getDestinationIPAddress(), connection.getDestinationPort(), method.name(), segmentNumber, request.getOffset()));
            sendPartition(partition, segmentNumber, request.getOffset());
        } else {
            hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER);
        }
    }

    private int getSegmentNumber(Request request, File partition) {
        int segmentNumber = partition.getSegmentNumber(request.getOffset());
        if (segmentNumber == -1) {
            //Exact offset not found. Getting the offset which is less than the given offset.
            int roundUpOffset = partition.getRoundUpOffset(request.getOffset());
            if (roundUpOffset != -1) {
                logger.debug(String.format("[%s:%d] [%s] Broker don't have exact offset %d. Sending information from %d offset instead", connection.getDestinationIPAddress(), connection.getDestinationPort(), method.name(),  request.getOffset(), roundUpOffset));
                request.setOffset(roundUpOffset);

                //Getting again segment number with new rounded offset
                segmentNumber = partition.getSegmentNumber(request.getOffset());
            } else {
                logger.warn(String.format("[%s:%d] [%s] No offset %d found for the topic %s - partition %d information.", connection.getDestinationIPAddress(), connection.getDestinationPort(), method.name(), request.getOffset(), request.getTopicName(), request.getPartition()));
            }
        }

        return segmentNumber;
    }

    private void sendPartition(File partition, int segmentNumber, int offset) {
        List<Segment> segments = partition.getSegmentsFrom(segmentNumber);

        for (Segment segment : segments) {
            int index = 0;

            if (segment.getSegment() == segmentNumber) {
                //Getting the index of the offset which contains the starting offset
                index = segment.getOffsetIndex(offset);
            }

            sendSegment(segment, index);

            if (!connection.isOpen()) {
                break;
            }
        }
    }

    private void sendSegment(Segment segment, int index) {
        try (FileInputStream stream = new FileInputStream(segment.getLocation())) {
            while (index < segment.getNumOfOffsets() && connection.isOpen()) {
                int length;
                int nextOffset;

                if (index + 1 < segment.getNumOfOffsets()) {
                    length = segment.getOffset(index + 1) - segment.getOffset(index);
                    nextOffset = segment.getOffset(index + 1);
                } else {
                    length = segment.getAvailableSize() - segment.getOffset(index);
                    nextOffset = segment.getAvailableSize();
                }

                byte[] data = new byte[length];
                int result = stream.read(data, 0, length);
                if(result == length) {
                    send(data, nextOffset);
                    logger.info(String.format("[%s:%d] [%s] Send %d number of bytes to the consumer %s:%d", connection.getSourceIPAddress(), connection.getSourcePort(), method.name(), data.length, connection.getDestinationIPAddress(), connection.getDestinationPort()));
                } else {
                    logger.warn(String.format("[%s] Not able to send data. Read %d number of bytes. Expected %d number of bytes.", method.name(), result, length));
                }

                index++;
            }
        } catch (IndexOutOfBoundsException | IOException ioException) {
            logger.error(String.format("Unable to open the segment file at the location %s.", segment.getLocation()), ioException);
        }
    }

    private void send(byte[] data, int nextOffset) {
        if (method == BrokerConstants.METHOD.PULL) {
            connection.send(BrokerPacketHandler.createDataPacket(data, nextOffset));
        } else {
            subscriber.onEvent(data);
        }
    }

    private void setMethod(Header.Content header) {
        if (header.getType() == BrokerConstants.TYPE.PULL.getValue()) {
            method = BrokerConstants.METHOD.PULL;
        } else {
            method = Constants.METHOD.PUSH;
        }
    }
}
