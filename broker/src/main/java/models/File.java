package models;

import configurations.BrokerConstants;
import controllers.FileManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class File {
    private static final Logger logger = LogManager.getLogger(File.class);
    private List<Segment> segments;
    private FileManager fileManager;
    private String parentLocation;
    private Segment segment;
    private long totalSize;           //Total size of messages received for the partition
    private int segmentsToRead;       //Total number of segments available to read
    private Timer timer;
    private volatile boolean isFlushed;

    public File() {
        segments = new ArrayList<>();
        fileManager = new FileManager();
    }

    public boolean initialize(String parentLocation, String topic, int partition) {
        this.parentLocation = String.format("%s/%s/%d", parentLocation, topic, partition);
        segment = new Segment(this.parentLocation, 0);

        return fileManager.createDirectory(parentLocation, String.format("%s/%d", topic, partition));
    }

    public synchronized void write(byte[] data) {
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
    }

    private synchronized void flush() {
        if (!isFlushed) {
            logger.debug(String.format("Either number of logs in the segment %d equal to the max %d or time-out happen. Flushing the segment %d to the disk.", segment.getNumOfLogs(), BrokerConstants.MAX_SEGMENT_MESSAGES, segment.getSegment()));

            if (segment.flush()) {
                logger.info(String.format("Flushed the segment %d to the disk. It is available to read", segment.getSegment()));
                segments.add(segment);
                segmentsToRead++;
            } else {
                //If failure happen while writing to the local disk then, data loss will happen.
                logger.warn(String.format("Fail while flushing segment %d to the disk. Data in the segment is lost and not available to read", segment.getSegment()));
            }

            segment = new Segment(parentLocation, segmentsToRead);

            isFlushed = true;
        }
    }
}
