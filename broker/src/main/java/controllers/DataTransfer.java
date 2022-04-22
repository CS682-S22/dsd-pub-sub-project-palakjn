package controllers;

import configurations.BrokerConstants;
import controllers.consumer.Subscriber;
import controllers.database.CacheManager;
import models.data.File;
import models.data.Segment;
import models.requests.TopicReadWriteRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.BrokerPacketHandler;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class DataTransfer {
    private static final Logger logger = LogManager.getLogger(DataTransfer.class);
    private Connection connection;
    private BrokerConstants.METHOD method;
    private Subscriber subscriber;
    private HostService hostService;
    private String receiver;

    public DataTransfer(Connection connection) {
        this.connection = connection;
        hostService = new HostService(logger);
    }

    /**
     * Set the mode of transfer
     */
    public void setMethod(BrokerConstants.METHOD method) {
        this.method = method;
    }

    /**
     * Set the subscriber to whom to transfer the data if the method is PUSH
     */
    public void setSubscriber(Subscriber subscriber) {
        this.subscriber = subscriber;
    }

    /**
     * Set the broker info which is going to receive the data
     */
    public void setReceiver(String receiver) {
        this.receiver = receiver;
    }

    /**
     * Send the logs if available from the requested offset. Send NACK if requested offset is more than the available data.
     */
    public void processRequest(TopicReadWriteRequest request) {
        File partition = CacheManager.getPartition(request.getName(), request.getPartition());

        int fromSegmentNumber = getSegmentNumber(request, partition, request.getFromOffset(), false);
        int toSegmentNumber = partition.getSegmentsToRead() - 1;

        if (method == BrokerConstants.METHOD.SYNC) {
            toSegmentNumber = getSegmentNumber(request, partition, request.getToOffset(), true);
        }

        if (fromSegmentNumber != -1 && toSegmentNumber != -1) {
            logger.debug(String.format("[%s] [%s] Sending data from segment %d to segment %d and from the offset %d", CacheManager.getBrokerInfo().getString(), method.name(), fromSegmentNumber, toSegmentNumber, request.getFromOffset()));
            sendPartition(request, partition, fromSegmentNumber, toSegmentNumber);
        } else {
            hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER);
        }
    }

    /**
     * Get the segment number which contains the requested offset. If exact offset don't exist then, return the segment number which contains the offset near to the given offset
     */
    private int getSegmentNumber(TopicReadWriteRequest request, File partition, int offset, boolean ciel) {
        int segmentNumber = partition.getSegmentNumber(offset, method == BrokerConstants.METHOD.SYNC);
        if (segmentNumber == -1) {
            //Exact offset not found. Getting the offset which is less than the given offset.
            int roundUpOffset = partition.getRoundUpOffset(offset, method == BrokerConstants.METHOD.SYNC, ciel);
            if (roundUpOffset != -1) {
                logger.debug(String.format("[%s:%d] [%s] Broker don't have exact offset %d. Sending information %s %d offset instead", connection.getDestinationIPAddress(), connection.getDestinationPort(), method != null ? method.name() : null,  offset, ciel ? "till" : "from", roundUpOffset));
                if (ciel) {
                    request.setToOffset(roundUpOffset);
                } else {
                    request.setFromOffset(roundUpOffset);
                }

                //Getting again segment number with new rounded offset
                segmentNumber = partition.getSegmentNumber(offset, method == BrokerConstants.METHOD.SYNC);
            } else {
                logger.warn(String.format("[%s:%d] [%s] No offset %d found for the topic %s - partition %d information.", connection.getDestinationIPAddress(), connection.getDestinationPort(), method != null ? method.name() : null, offset, request.getName(), request.getPartition()));
            }
        }

        return segmentNumber;
    }

    /**
     * Get all the available segments to read from the segment number which contains the offset.
     * If the method is PUSH then, send all the logs from the available segments.
     * If the method is PULL then, send only the requested amount of logs.
     */
    private void sendPartition(TopicReadWriteRequest request, File partition, int fromSegmentNumber, int toSegmentNumber) {
        List<Segment> segments = partition.getSegmentsFrom(fromSegmentNumber, toSegmentNumber);
        int count = 0;
        int offset;

        //TODO: Remove
        logger.debug(String.format("[%s] Sending data from %d segments.", CacheManager.getBrokerInfo().getString(), segments.size()));

        for (Segment segment : segments) {
            int offsetIndex = 0;

            if (segment.getSegment() == fromSegmentNumber) {
                //Getting the index of the offset which contains the starting offset
                offsetIndex = segment.getOffsetIndex(request.getFromOffset());
            }

            while (offsetIndex < segment.getNumOfOffsets() && (method != BrokerConstants.METHOD.PULL || count < request.getNumOfMsg())) {

                if (method == BrokerConstants.METHOD.SYNC && segment.getSegment() == toSegmentNumber && offsetIndex > segment.getOffsetIndex(request.getToOffset())) {
                    //Have to send only within the range during sync process
                    //TODO: REMOVE
                    logger.debug(String.format("[%s] Not sending offset %d as offsetIndex %d exceed toOffset %d when the segment number is %d", CacheManager.getBrokerInfo().getString(), segment.getOffset(offsetIndex), offsetIndex, segment.getOffsetIndex(request.getToOffset()), toSegmentNumber));
                    break;
                }

                //TODO: Remove
                logger.debug(String.format("[%s] Sending data with offset %d from segment %d.", CacheManager.getBrokerInfo().getString(), segment.getOffset(offsetIndex), segment.getSegment()));

                offset = segment.getOffset(offsetIndex) - segment.getOffset(0);
                sendSegment(segment, offsetIndex, offset, !segment.isFlushed(), partition.getName(), request.getToOffset());
                count++;
                offsetIndex++;
            }
        }
    }

    /**
     * Reads one log from segment file and send to the consumer
     */
    private void sendSegment(Segment segment, int offsetIndex, int offset, boolean readBuffer, String key, int toOffset) {
        int length;
        int nextOffset;

        if (offsetIndex + 1 < segment.getNumOfOffsets()) {
            length = segment.getOffset(offsetIndex + 1) - segment.getOffset(offsetIndex);
            nextOffset = segment.getOffset(offsetIndex + 1);
        } else {
            length = segment.getAvailableSize() - segment.getOffset(offsetIndex);
            nextOffset = segment.getAvailableSize();
        }

        byte[] data;
        if (readBuffer) {
            data = segment.getLog(offsetIndex, length);
        } else {
            data = new byte[length];

            try (FileInputStream stream = new FileInputStream(segment.getLocation())) {
                stream.getChannel().position(offset);
                int result = stream.read(data);
                if(result != length) {
                    logger.warn(String.format("[%s] Not able to send data. Read %d number of bytes. Expected %d number of bytes.", method.name(), result, length));
                    data = null;
                }
            } catch (IndexOutOfBoundsException | IOException ioException) {
                logger.error(String.format("Unable to open the segment file at the location %s.", segment.getLocation()), ioException);
                data = null;
            }
        }

        if (data != null) {
            send(key, data, nextOffset, toOffset);
            logger.info(String.format("[%s] [%s] Send %d number of bytes to the consumer/broker %s:%d", CacheManager.getBrokerInfo().getString(), method.name(), data.length, connection.getDestinationIPAddress(), connection.getDestinationPort()));
        }
    }

    /**
     * Sends the data to the consumer with the next offset
     */
    private void send(String key, byte[] data, int nextOffset, int toOffset) {
        if (method == BrokerConstants.METHOD.PULL) {
            connection.send(BrokerPacketHandler.createDataPacket(data, nextOffset));
        } else if (method == BrokerConstants.METHOD.PUSH) {
            subscriber.onEvent(data);
        } else if (method == BrokerConstants.METHOD.SYNC) {
            byte[] dataPacket = BrokerPacketHandler.createDataPacket(key, BrokerConstants.DATA_TYPE.CATCH_UP_DATA, data, toOffset);

            Broker broker = CacheManager.getBroker(key, receiver);
            if (broker != null) {
                logger.info(String.format("[%s] Sending %d bytes of data to the receiver %s", CacheManager.getBrokerInfo().getString(), data.length, broker.getString()));
                broker.send(dataPacket, BrokerConstants.CHANNEL_TYPE.DATA, BrokerConstants.ACK_WAIT_TIME, true);
            }
        }
    }
}
