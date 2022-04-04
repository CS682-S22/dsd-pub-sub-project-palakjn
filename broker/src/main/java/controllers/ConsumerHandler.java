package controllers;

import configurations.BrokerConstants;
import models.File;
import models.Header;
import models.Segment;
import models.requests.TopicReadWriteRequest;
import models.requests.Request;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.BrokerPacketHandler;
import utilities.JSONDesrializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

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
    private BrokerConstants.METHOD method;

    public ConsumerHandler(Connection connection) {
        this.connection = connection;
        hostService = new HostService(logger);
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
                Request<TopicReadWriteRequest> request = JSONDesrializer.fromJson(body, Request.class);

                if (request != null && validateRequest(header, request.getRequest())) {
                    if (header.getType() == BrokerConstants.TYPE.PULL.getValue()) {
                        method = BrokerConstants.METHOD.PULL;
                        processPullRequest(request.getRequest());

                        connection.closeConnection();
                    } else {
                        //Adding subscriber
                        subscriber = new Subscriber(connection);
                        CacheManager.addSubscriber(subscriber);

                        method = BrokerConstants.METHOD.PUSH;
                        processRequest(request.getRequest());
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
            logger.debug(String.format("[%s:%d] Received [%s] request to get the data of topic %s - partition %d - offset - %d from the consumer %s:%d", connection.getSourceIPAddress(), connection.getSourcePort(), BrokerConstants.findTypeByValue(header.getType()), request.getName(), request.getPartition(), request.getOffset(), connection.getDestinationIPAddress(), connection.getDestinationPort()));

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
            processRequest(request);

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
                        Request<TopicReadWriteRequest> request = JSONDesrializer.fromJson(body, Request.class);

                        if (request != null && validateRequest(header, request.getRequest())) {
                            readTopicRequest = request.getRequest();
                        }
                    }
                }
            }
        }

        return readTopicRequest;
    }

    /**
     * Send the logs if available from the requested offset. Send NACK if requested offset is more than the available data.
     */
    private void processRequest(TopicReadWriteRequest request) {
        File partition = CacheManager.getPartition(request.getName(), request.getPartition());

        int segmentNumber = getSegmentNumber(request, partition);

        if (segmentNumber != -1) {
            logger.debug(String.format("[%s:%d] [%s] Segment %d holding information of %d offset", connection.getDestinationIPAddress(), connection.getDestinationPort(), method.name(), segmentNumber, request.getOffset()));
            sendPartition(request, partition, segmentNumber);
        } else {
            hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER);
        }
    }

    /**
     * Get the segment number which contains the requested offset. If exact offset don't exist then, return the segment number which contains the offset which is just less than the given offset
     */
    private int getSegmentNumber(TopicReadWriteRequest request, File partition) {
        int segmentNumber = partition.getSegmentNumber(request.getOffset());
        if (segmentNumber == -1) {
            //Exact offset not found. Getting the offset which is less than the given offset.
            int roundUpOffset = partition.getRoundUpOffset(request.getOffset());
            if (roundUpOffset != -1) {
                logger.debug(String.format("[%s:%d] [%s] Broker don't have exact offset %d. Sending information from %d offset instead", connection.getDestinationIPAddress(), connection.getDestinationPort(), method != null ? method.name() : null,  request.getOffset(), roundUpOffset));
                request.setOffset(roundUpOffset);

                //Getting again segment number with new rounded offset
                segmentNumber = partition.getSegmentNumber(request.getOffset());
            } else {
                logger.warn(String.format("[%s:%d] [%s] No offset %d found for the topic %s - partition %d information.", connection.getDestinationIPAddress(), connection.getDestinationPort(), method != null ? method.name() : null, request.getOffset(), request.getName(), request.getPartition()));
            }
        }

        return segmentNumber;
    }

    /**
     * Get all the available segments to read from the segment number which contains the offset.
     * If the method is PUSH then, send all the logs from the available segments.
     * If the method is PULL then, send only the requested amount of logs.
     */
    private void sendPartition(TopicReadWriteRequest request, File partition, int segmentNumber) {
        List<Segment> segments = partition.getSegmentsFrom(segmentNumber);
        int count = 0;

        for (Segment segment : segments) {
            int offsetIndex = 0;

            if (segment.getSegment() == segmentNumber) {
                //Getting the index of the offset which contains the starting offset
                offsetIndex = segment.getOffsetIndex(request.getOffset());
            }

            while (offsetIndex < segment.getNumOfOffsets() && (method == BrokerConstants.METHOD.PUSH || (method == BrokerConstants.METHOD.PULL && count < request.getNumOfMsg()))) {
                sendSegment(segment, offsetIndex);
                count++;
                offsetIndex++;
            }
        }
    }

    /**
     * Reads one log from segment file and send to the consumer
     */
    private void sendSegment(Segment segment, int offsetIndex) {
        try (FileInputStream stream = new FileInputStream(segment.getLocation())) {
            int length;
            int nextOffset;

            if (offsetIndex + 1 < segment.getNumOfOffsets()) {
                length = segment.getOffset(offsetIndex + 1) - segment.getOffset(offsetIndex);
                nextOffset = segment.getOffset(offsetIndex + 1);
            } else {
                length = segment.getAvailableSize() - segment.getOffset(offsetIndex);
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
        } catch (IndexOutOfBoundsException | IOException ioException) {
            logger.error(String.format("Unable to open the segment file at the location %s.", segment.getLocation()), ioException);
        }
    }

    /**
     * Sends the data to the consumer with the next offset
     */
    private void send(byte[] data, int nextOffset) {
        if (method == BrokerConstants.METHOD.PULL) {
            connection.send(BrokerPacketHandler.createDataPacket(data, nextOffset));
        } else {
            subscriber.onEvent(data);
        }
    }
}
