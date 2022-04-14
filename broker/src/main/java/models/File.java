package models;

import configurations.BrokerConstants;
import controllers.FileManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
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
    private int totalSize;           //Total size of messages received for the partition
    private int segmentsToRead;       //Total number of segments available to read
    private int availableSize;
    private Timer timer;
    private volatile boolean isFlushed;
    private ReentrantReadWriteLock lock;

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
     * Writes new data to the segment.
     * Flush the segment either after certain amount of time elapsed or the number of logs holding exceeded the MAX allowed number of logs.
     */
    public void write(byte[] data) {
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
        totalSize += data.length;

        if (segment.getNumOfLogs() == BrokerConstants.MAX_SEGMENT_MESSAGES) {
            flush();
            timer.cancel();
        }

        lock.writeLock().unlock();
    }

    /**
     * Getting the segment number which is holding the offset
     */
    public int getSegmentNumber(int offset) {
        lock.readLock().lock();
        int segmentNumber = -1;

        if (offset < availableSize) {
            for (Segment seg : segments) {
                if (offset < seg.getAvailableSize() && seg.isOffsetExist(offset)) {
                    segmentNumber = seg.getSegment();
                    break;
                }
            }
        }

        lock.readLock().unlock();
        return segmentNumber;
    }

    /**
     * Getting the offset which is less than the given the offset.
     */
    public int getRoundUpOffset(int offset) {
        lock.readLock().lock();
        int roundUpOffset = -1;

        if (offset < availableSize) {
            for (Segment segment : segments) {
                if (offset < segment.getAvailableSize()) {
                    roundUpOffset = segment.getRoundUpOffset(offset);

                    if (roundUpOffset != -1) {
                        break;
                    }
                }
            }
        }

        lock.readLock().unlock();
        return roundUpOffset;
    }

    /**
     * Get all the available segment from the given number
     */
    public List<Segment> getSegmentsFrom(int segmentNumber) {
        List<Segment> segments = new ArrayList<>();
        lock.readLock().lock();

        for (int i = segmentNumber; i < this.segments.size(); i++) {
            segments.add(this.segments.get(i));
        }

        lock.readLock().unlock();
        return segments;
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
