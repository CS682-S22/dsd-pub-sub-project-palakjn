package models;

import controllers.FileManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Segment {
    private int segment;
    private byte[] buffer;
    private List<Long> offsets;
    private String location;
    private long size;
    private FileManager fileManager;

    public Segment(String parentLocation, int segment) {
        this.segment = segment;
        this.offsets = new ArrayList<>();
        this.location = String.format("%s/%d.log", parentLocation, segment);
        this.fileManager = new FileManager();
    }

    public int getSegment() {
        return segment;
    }

    public void addOffset(long offset) {
        offsets.add(offset);
    }

    public boolean isOffsetExist(long offset) {
        return offsets.contains(offset);
    }

    public void write(byte[] data) {
        if (buffer == null) {
            buffer = data;
        } else {
            buffer = ByteBuffer.allocate(buffer.length + data.length).put(buffer).put(data).array();
        }

        size += data.length;
    }

    public boolean flush() {
        boolean isSuccess = false;

        isSuccess = fileManager.write(buffer, location);
        buffer = null;

        return isSuccess;
    }

    public long getSize() {
        return size;
    }
}
