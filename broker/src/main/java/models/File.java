package models;

import configurations.BrokerConstants;
import controllers.FileManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class File {
    private static final Logger logger = LogManager.getLogger(File.class);
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

    public File() {
        segments = new ArrayList<>();
        fileManager = new FileManager();
        lock = new ReentrantReadWriteLock();
    }

    public boolean initialize(String parentLocation, String topic, int partition) {
        this.parentLocation = String.format("%s/%s/%d", parentLocation, topic, partition);
        segment = new Segment(this.parentLocation, 0);

        return fileManager.createDirectory(parentLocation, String.format("%s/%d", topic, partition));
    }

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

    public int getSegmentNumber(int offset) {
        lock.readLock().lock();
        int segmentNumber = -1;

        if (offset < availableSize) {
            for (Segment seg : segments) {
                if (seg.isOffsetExist(offset)) {
                    segmentNumber = seg.getSegment();
                    break;
                }
            }
        }

        lock.readLock().unlock();

        return segmentNumber;
    }

    public List<Segment> getSegmentsFrom(int segmentNumber) {
        List<Segment> segments = new ArrayList<>();
        lock.readLock().lock();

        if (segmentNumber > 0) {
            for (int i = segmentNumber; i < this.segments.size(); i++) {
                segments.add(this.segments.get(i));
            }
        }

        lock.readLock().unlock();
        return segments;
    }

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

            segment = new Segment(parentLocation, segmentsToRead);

            isFlushed = true;
        }

        lock.writeLock().unlock();
    }
}
