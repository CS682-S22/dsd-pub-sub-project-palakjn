package models;

import configurations.BrokerConstants;
import controllers.FileManager;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import utilities.NodeTimer;

import java.util.ArrayList;
import java.util.List;

public class File {
    private static final Logger logger = LogManager.getLogger(File.class);
    private List<Segment> segments;
    private FileManager fileManager;
    private String parentLocation;
    private Segment segment;
    private long totalSize;           //Total size of messages received for the partition
    private int segmentsToRead;       //Total number of segments available to read
    private NodeTimer timer;          //Timer to keep the segment for writing data. After timeout, it will be flush to the disk

    public File() {
        segments = new ArrayList<>();
        fileManager = new FileManager();
        timer = new NodeTimer();
    }

    public boolean initialize(String parentLocation, String topic, int partition) {
        this.parentLocation = String.format("%s/%s/%d", parentLocation, topic, partition);
        segment = new Segment(this.parentLocation, 0);
        timer.startTimer("Segment", BrokerConstants.SEGMENT_FLUSH_TIME);
        return fileManager.createDirectory(parentLocation, String.format("%s/%d", topic, partition));
    }

    public synchronized void write(byte[] data) {
        if (data.length > (BrokerConstants.MAX_SEGMENT_SIZE - segment.getSize()) || timer.isTimeout()) {
            logger.debug(String.format("Either new data length %d exceed available size %d or time-out happen: %b. Flushing the segment %d to the disk.", data.length, BrokerConstants.MAX_SEGMENT_SIZE - segment.getSize(), timer.isTimeout(), segment.getSegment()));

            if (segment.flush()) {
                logger.info(String.format("Flushed the segment %d to the disk. It is available to read", segment.getSegment()));
                segments.add(segment);
                segmentsToRead++;
            } else {
                //If failure happen while writing to the local disk then, data loss will happen.
                logger.warn(String.format("Fail while flushing segment %d to the disk. Data in the segment is lost and not available to read", segment.getSegment()));
            }

            segment = new Segment(parentLocation, segmentsToRead + 1);
            timer.stopTimer();
            timer.startTimer("Segment", BrokerConstants.SEGMENT_FLUSH_TIME);
        }

        segment.write(data);
        segment.addOffset(totalSize);
        totalSize += data.length;
    }
}
