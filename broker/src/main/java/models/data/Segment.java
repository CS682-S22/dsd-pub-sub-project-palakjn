package models.data;

import controllers.database.CacheManager;
import controllers.database.FileManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Responsible for holding segment information's like buffer, offsets.
 *
 * @author Palak Jain
 */
public class Segment {
    private static final Logger logger = LogManager.getLogger(Segment.class);
    private int segment;
    private byte[] buffer;
    private List<Integer> offsets;
    private String location;
    private int numOfLogs;
    private FileManager fileManager;
    private int availableSize; //Will hold size of the current segment + sum of the size of previous segments
    private boolean isFlushed;

    public Segment(String parentLocation, int segment, int availableSize) {
        this.segment = segment;
        this.offsets = new ArrayList<>();
        this.location = String.format("%s/%d.log", parentLocation, segment);
        this.fileManager = new FileManager();
        this.availableSize = availableSize;
    }

    public Segment(Segment segment) {
        this.segment = segment.getSegment();
        this.buffer = segment.getBuffer();
        this.offsets = segment.offsets;
        this.location = segment.getLocation();
        this.numOfLogs = segment.getNumOfLogs();
        this.fileManager = segment.fileManager;;
        this.availableSize = segment.getAvailableSize();
    }

    /**
     * Get the log from the buffer from the given offset of given length
     */
    public byte[] getLog(int offset, int length) {
        byte[] data = new byte[length];

        System.arraycopy(buffer, offset, data, 0, length);

        return data;
    }

    /**
     * Get the current segment
     */
    public int getSegment() {
        return segment;
    }

    /**
     * Add new offset to the collection
     */
    public void addOffset(int offset) {
        offsets.add(offset);
    }

    /**
     * Checks if the given offset exists
     */
    public boolean isOffsetExist(int offset) {
        //TODO: Remove
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(String.format("[%s] The segment %d has offsets: ", CacheManager.getBrokerInfo().getString(), segment));

        for (int off : offsets) {
            stringBuilder.append(off).append(", ");
        }

        logger.debug(stringBuilder.toString());

        return offsets.contains(offset);
    }

    /**
     * If ciel is false then get the offset which is less than or equal to the given offset
     * If ciel is true then get the offset which is more than or equal to the given offset
     */
    public int getRoundUpOffset(int initialOffset, boolean ciel) {
        int roundUpOffset = -1;

        for (int offset : offsets) {
            if (!ciel && initialOffset < offset) {
                break;
            } else if (ciel && initialOffset > offset) {
                roundUpOffset = offset;
                continue;
            }

            roundUpOffset = offset;
        }

        return roundUpOffset;
    }

    /**
     * Get the index which is holding the offset
     */
    public int getOffsetIndex(int offset) {
        return offsets.indexOf(offset);
    }

    /**
     * Gets the number of offsets the segment has
     */
    public int getNumOfOffsets() {
        return offsets.size();
    }

    /**
     * Gets the offset at the given index.
     * If the given index if OutOfBound then returns -1
     */
    public int getOffset(int index) {
        if (index < offsets.size()) {
            return offsets.get(index);
        } else {
            return -1;
        }
    }

    /**
     * Write data to the local buffer
     */
    public void write(byte[] data) {
        if (buffer == null) {
            buffer = data;
        } else {
            buffer = ByteBuffer.allocate(buffer.length + data.length).put(buffer).put(data).array();
        }

        availableSize += data.length;
        numOfLogs++;
    }

    /**
     * Write the buffer data to the file
     */
    public boolean flush() {
        boolean isSuccess;

        isSuccess = fileManager.write(buffer, location);
        buffer = null;
        isFlushed = true;

        return isSuccess;
    }

    /**
     * Determines if the segment is flushed
     */
    public boolean isFlushed() {
        return isFlushed;
    }

    /**
     * Get the buffer
     */
    public byte[] getBuffer() {
        return buffer;
    }

    /**
     * Checks if segment holding any data or not
     */
    public boolean isEmpty() {
        return buffer == null;
    }

    /**
     * Gets the number of logs written to the current segment
     */
    public int getNumOfLogs() {
        return numOfLogs;
    }

    /**
     * Gets the file location of the segment
     */
    public String getLocation() {
        return location;
    }

    /**
     * Gets the total size of the data written to current segment plus the total size of the data written to previous segments.
     */
    public int getAvailableSize() {
        return availableSize;
    }
}
