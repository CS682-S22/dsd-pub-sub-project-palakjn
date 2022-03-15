package models;

import controllers.FileManager;

import java.util.ArrayList;
import java.util.List;

public class File {
    private List<Segment> segments;
    private FileManager fileManager;
    private long totalSize;
    private int segmentToWrite;
    private int segmentsToRead;
    private long curSegmentSize;

    public File(String location, String topicName, int partition) {
        segments = new ArrayList<>();
        fileManager = new FileManager(location);

        //Create directory with the name as topic_partition
        //Create one file inside the directory with the name as 0.log
    }

    public void write(byte[] data) {
        //Append the data to the file
        //Add totalSize as the offset to the current segment
        //add data.length to the totalSize
        //add data.length to the curSegmentSize
    }

    public void createNewSegment() {
        segmentsToRead++;
        curSegmentSize = 0;
        segmentToWrite++;

        Segment segment = new Segment();
        segments.add(segment);

        //Create new file with the name as segmentToWrite (Which will make stream pointing to new file)
    }
}
