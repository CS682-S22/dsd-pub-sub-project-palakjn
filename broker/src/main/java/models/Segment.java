package models;

import controllers.FileManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Responsible for holding segment information's like buffer, offsets.
 *
 * @author Palak Jain
 */
public class Segment {
    private int segment;
    private byte[] buffer;
    private List<Integer> offsets;
    private String location;
    private int numOfLogs;
    private FileManager fileManager;
    private int availableSize; //Will hold size of the current segment + sum of the size of previous segments

    public Segment(String parentLocation, int segment, int availableSize) {
        this.segment = segment;
        this.offsets = new ArrayList<>();
        this.location = String.format("%s/%d.log", parentLocation, segment);
        this.fileManager = new FileManager();
        this.availableSize = availableSize;
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
        return offsets.contains(offset);
    }

    /**
     * Get the offset which is less than or equal to the given offset
     */
    public int getRoundUpOffset(int initialOffset) {
        int roundUpOffset = -1;

        for (int offset : offsets) {
            if (initialOffset >= offset) {
                roundUpOffset = offset;
            } else {
                break;
            }
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

        return isSuccess;
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
