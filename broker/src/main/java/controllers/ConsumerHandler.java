package controllers;

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
             if (header.getType() == BrokerConstants.TYPE.PULL.getValue()) {
                logger.info(String.format("[%s:%d] Received pull request from the consumer.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                hostService.sendACK(connection, BrokerConstants.REQUESTER.CONSUMER, header.getSeqNum());
                //Pull request
                processPullRequest(body);

                //Closing the connection after sending data
                connection.closeConnection();
            }
        }
    }

    private void processPullRequest(byte[] body) {
        Request request = JSONDesrializer.fromJson(body, Request.class);

        if (request != null && request.isValid()) {
            logger.debug(String.format("[%s:%d] Received request to get the data of topic %s - partition %d - offset - %d", connection.getDestinationIPAddress(), connection.getDestinationPort(), request.getTopicName(), request.getPartition(), request.getOffset()));
            if (CacheManager.isExist(request.getTopicName(), request.getPartition())) {
                File partition = CacheManager.getPartition(request.getTopicName(), request.getPartition());

                boolean toSend = true;

                while (toSend) {
                    //Getting the segment number which is holding the exact offset
                    int segmentNumber = partition.getSegmentNumber(request.getOffset());
                    if (segmentNumber != -1) {
                        logger.debug(String.format("[%s:%d] Segment %d holding information of %d offset", connection.getDestinationIPAddress(), connection.getDestinationPort(), segmentNumber, request.getOffset()));
                        send(partition, segmentNumber, request.getOffset());
                        toSend = false;
                    } else {
                        //Exact offset not found. Getting the offset which is more than the given offset.
                        int roundUpOffset = partition.getRoundUpOffset(request.getOffset());
                        if (roundUpOffset != -1) {
                            logger.debug(String.format("[%s:%d] Broker don't have exact offset %d. Sending information from %d offset instead", connection.getDestinationIPAddress(), connection.getDestinationPort(), request.getOffset(), roundUpOffset));
                            request.setOffset(roundUpOffset);
                        } else {
                            logger.warn(String.format("[%s:%d] No offset %d found for the topic %s - partition %d information.", connection.getDestinationIPAddress(), connection.getDestinationPort(), request.getOffset(), request.getTopicName(), request.getPartition()));
                            hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER);
                            toSend = false;
                        }
                    }
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
                        if(connection.send(BrokerPacketHandler.createDataPacket(data, nextOffset))) {
                            logger.debug(String.format("Send %d number of bytes to the consumer. Next offset is %d", result, nextOffset));
                        } else if (!connection.isOpen()) {
                            logger.warn("Connection is closed by the consumer.");
                            break;
                        }
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
