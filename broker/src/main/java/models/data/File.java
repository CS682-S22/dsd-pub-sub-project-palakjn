package models.data;

import configurations.BrokerConstants;
import controllers.Brokers;
import controllers.database.CacheManager;
import controllers.database.FileManager;
import models.sync.DataPacket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.BrokerPacketHandler;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Responsible for holding the partition information of the topic.
 *
 * @author Palak Jain
 */
public class File {
    private static final Logger logger = LogManager.getLogger(File.class);
    private String name;
    private List<Segment> segments;
    private FileManager fileManager;
    private String parentLocation;
    private Segment segment;
    private int totalSize;            //Total size of messages received for the partition
    private int segmentsToRead;       //Total number of segments available to read
    private int availableSize;        //Size of the messages which are available to read
    private int offset;               //Offset of the last message received
    private Timer timer;
    private volatile boolean isFlushed;
    private ReentrantReadWriteLock lock;
    private List<byte[]> buffer;

    public File(String topicName, int partition) {
        name = String.format("%s:%d", topicName, partition);
        segments = new ArrayList<>();
        fileManager = new FileManager();
        lock = new ReentrantReadWriteLock();
    }

    /**
     * Initialize with the topic name, partition, location where the segments will be created.
     */
    public boolean initialize(String parentLocation, String topic, int partition) {
        this.parentLocation = String.format("%s/%s/%d", parentLocation, topic, partition);
        segment = new Segment(this.parentLocation, 0, 0);

        return fileManager.createDirectory(parentLocation, String.format("%s/%d", topic, partition));
    }

    /**
     * Get the name of the file aka partition
     */
    public String getName() {
        return name;
    }

    /**
     * Get the expected offset of next log
     */
    public int getTotalSize() {
        return totalSize;
    }

    /**
     * Writes new data to the segment.
     * Flush the segment either after certain amount of time elapsed or the number of logs holding exceeded the MAX allowed number of logs.
     */
    public boolean write(byte[] data, boolean sendToFollowers) {
        boolean isSuccess = true;
        lock.writeLock().lock();

        if (segment.isEmpty()) {
            //start timer when writing to the new segment
            timer = new Timer();
            TimerTask task = new TimerTask() {
                public void run() {
                    logger.debug("Time-out happen. Will flush the segment");
                    timer.cancel();
                    this.cancel();

                    flush();
                }
            };

            timer.schedule(task, BrokerConstants.SEGMENT_FLUSH_TIME);
            isFlushed = false;
        }

        segment.write(data);
        segment.addOffset(totalSize);
        offset = totalSize;
        totalSize += data.length;

        if (segment.getNumOfLogs() == BrokerConstants.MAX_SEGMENT_MESSAGES) {
            flush();
            timer.cancel();
        }

        if (sendToFollowers) {
            Brokers brokers = CacheManager.getBrokers(name);

            if (brokers != null) {
                data = BrokerPacketHandler.createDataPacket(name, BrokerConstants.DATA_TYPE.REPLICA_DATA, data);
                isSuccess = brokers.send(data, BrokerConstants.CHANNEL_TYPE.DATA, BrokerConstants.ACK_WAIT_TIME, true);

                if (isSuccess) {
                    logger.info(String.format("[%s:%d] Send the data to %d number of followers successfully.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort(), brokers.getSize()));
                } else {
                    logger.info(String.format("[%s:%d] Fail to send the data to one or more number of %d followers.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort(), brokers.getSize()));
                }
            }
        }

        lock.writeLock().unlock();
        return isSuccess;
    }

    /**
     * Write the data received from another broker to local segment based on the current broker status for the partition
     */
    public boolean write(DataPacket dataPacket) {
        boolean isWritten = true;
        lock.writeLock().lock();

        BrokerConstants.BROKER_STATE broker_state = CacheManager.getStatus(dataPacket.getKey());

        if (broker_state == BrokerConstants.BROKER_STATE.READY) {
            if (Objects.equals(dataPacket.getDataType(), BrokerConstants.DATA_TYPE.REPLICA_DATA)) {
                logger.info(String.format("[%s:%d] Writing REPLICA data to segment for key %s", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort(), name));
                write(dataPacket.getData(), false);
            } else {
                logger.warn(String.format("[%s:%d] Not writing data to segment as received %s request type when the broker is in READY state for key %s.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort(), dataPacket.getDataType(), name));
                isWritten = false;
            }
        } else if (broker_state == BrokerConstants.BROKER_STATE.SYNC) {
            if (Objects.equals(dataPacket.getDataType(), BrokerConstants.DATA_TYPE.REPLICA_DATA)) {
                logger.info(String.format("[%s:%d] Writing the REPLICA data to local buffer as the broker state is SYNC for key %s.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort(), name));
                writeToLocal(dataPacket.getData());
            } else if (Objects.equals(dataPacket.getDataType(), BrokerConstants.DATA_TYPE.CATCH_UP_DATA)) {
                logger.info(String.format("[%s:%d] Writing the CATCH-UP data to segment as the broker state is SYNC for key %s.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort(), name));
                write(dataPacket.getData(), false);
            } else {
                logger.warn(String.format("[%s:%d] Not writing data to segment as received %s request type when the broker is in SYNC state for key %s.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort(), dataPacket.getDataType(), name));
                isWritten = false;
            }
        } else {
            logger.warn(String.format("[%s:%d] Not writing data to segment when the broker is in %s state for key %s.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort(), broker_state.name(), name));
            isWritten = false;
        }

        lock.writeLock().unlock();
        return isWritten;
    }

    /**
     * Write the data to local buffer
     */
    private void writeToLocal(byte[] data) {
        lock.writeLock().lock();

        if (buffer == null) {
            buffer = new ArrayList<>();
        }

        buffer.add(data);
        lock.writeLock().unlock();
    }

    /**
     * Flush the local buffer content to the storage
     */
    public void flushToSegment() {
        lock.writeLock().lock();

        if (buffer != null) {
            for (byte[] data : buffer) {
                write(data, false);
            }

            buffer = null;
        }

        lock.writeLock().unlock();
    }

    /**
     * Get the offset of the last message received for the partition
     */
    public int getOffset() {
        lock.readLock().lock();

        try {
            return offset;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Getting the segment number which is holding the offset
     */
    public int getSegmentNumber(int offset, boolean isSync) {
        lock.readLock().lock();
        int segmentNumber = -1;

        if (offset < availableSize) {
            for (Segment seg : segments) {
                if (offset < seg.getAvailableSize() && seg.isOffsetExist(offset)) {
                    segmentNumber = seg.getSegment();
                    break;
                }
            }
        } else if (isSync && offset < segment.getAvailableSize() && segment.isOffsetExist(segmentNumber)) {
            segmentNumber = segment.getSegment();
        }

        lock.readLock().unlock();
        return segmentNumber;
    }

    /**
     * If ciel is false then get the offset which is less than or equal to the given offset
     * If ciel is true then get the offset which is more than or equal to the given offset
     */
    public int getRoundUpOffset(int offset, boolean isSync, boolean ciel) {
        lock.readLock().lock();
        int roundUpOffset = -1;

        if (offset < availableSize) {
            for (Segment segment : segments) {
                if (offset < segment.getAvailableSize()) {
                    roundUpOffset = segment.getRoundUpOffset(offset, ciel);

                    if (roundUpOffset != -1) {
                        break;
                    }
                }
            }
        } else if (isSync && offset < segment.getAvailableSize()) {
            roundUpOffset = segment.getRoundUpOffset(offset, ciel);
        }

        lock.readLock().unlock();
        return roundUpOffset;
    }

    /**
     * Get all the available segment within the given range
     */
    public List<Segment> getSegmentsFrom(int from, int to) {
        List<Segment> segments = new ArrayList<>();
        lock.readLock().lock();

        if (to < this.segments.size()) {
            for (int i = from; i < to; i++) {
                segments.add(this.segments.get(i));
            }
        } else if (to == this.segments.size()){
            for (int i = from; i < this.segments.size(); i++) {
                segments.add(this.segments.get(i));
            }

            segments.add(new Segment(segment));
        }

        lock.readLock().unlock();
        return segments;
    }

    /**
     * Get the total number of segments to read from
     */
    public int getSegmentsToRead() {
        lock.readLock().lock();

        try {
            return segmentsToRead;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Flush the segment to the disk and create new segment to write to for future data
     */
    private void flush() {
        lock.writeLock().lock();

        if (!isFlushed) {
            logger.debug(String.format("Either number of logs in the segment %d equal to the max %d or time-out happen. Flushing the segment %d to the disk.", segment.getNumOfLogs(), BrokerConstants.MAX_SEGMENT_MESSAGES, segment.getSegment()));

            if (segment.flush()) {
                logger.info(String.format("Flushed the segment %d to the disk. It is available to read", segment.getSegment()));
                availableSize = totalSize;
                segments.add(segment);
                segmentsToRead++;
            } else {
                //If failure happen while writing to the local disk then, data loss will happen.
                logger.warn(String.format("Fail while flushing segment %d to the disk. Data in the segment is lost and not available to read", segment.getSegment()));
            }

            segment = new Segment(parentLocation, segmentsToRead, totalSize);

            isFlushed = true;
        }

        lock.writeLock().unlock();
    }
}
