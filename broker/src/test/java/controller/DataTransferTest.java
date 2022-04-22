package controller;

import configuration.Constants;
import controllers.Connection;
import controllers.DataTransfer;
import controllers.database.CacheManager;
import models.Host;
import models.data.File;
import models.data.Segment;
import models.requests.TopicReadWriteRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class DataTransferTest {
    @BeforeAll
    public static void init() {
        CacheManager.setBroker(new Host("localhost", 1700));
    }

    /**
     * Test whether getSegmentNumber() returns the segment number if exact given fromOffset exist
     */
    @Test
    public void getSegmentNumber_exactFromOffset_returnSegment() {
        TopicReadWriteRequest request = new TopicReadWriteRequest("topic", 1, 15, 15);
        File partition = new File("topic", 1);

        setup(partition);

        DataTransfer dataTransfer = new DataTransfer(new Connection());
        int actual = -1;

        try {
            Method processMethod = DataTransfer.class.getDeclaredMethod("getSegmentNumber", TopicReadWriteRequest.class, File.class, int.class, boolean.class);
            processMethod.setAccessible(true);
            actual = (int) processMethod.invoke(dataTransfer, request, partition, request.getFromOffset(), false);

        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException exception) {
            System.err.println(exception.getMessage());
        }

        Assertions.assertEquals(0, actual);
    }

    /**
     * Test whether getSegmentNumber return the segment number if the exact offset don't exist but the offset which is more
     * than the given offset exists
     */
    @Test
    public void getSegmentNumber_roundOffset_cielFalse_returnSegment() {
        TopicReadWriteRequest request = new TopicReadWriteRequest("topic", 1, 28, 15);
        File partition = new File("topic", 1);

        setup(partition);

        DataTransfer dataTransfer = new DataTransfer(new Connection());
        dataTransfer.setMethod(Constants.METHOD.PULL);
        int actual = -1;

        try {
            Method processMethod = DataTransfer.class.getDeclaredMethod("getSegmentNumber", TopicReadWriteRequest.class, File.class, int.class, boolean.class);
            processMethod.setAccessible(true);
            actual = (int) processMethod.invoke(dataTransfer, request, partition, request.getFromOffset(), false);

        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException exception) {
            System.err.println(exception.getMessage());
        }

        Assertions.assertEquals(1, actual);
    }

    /**
     * Test whether getSegmentNumber return the segment number if the exact offset don't exist but the offset which is less
     * than the given offset exists
     */
    @Test
    public void getSegmentNumber_roundOffset_cielTrue_returnSegment() {
        TopicReadWriteRequest request = new TopicReadWriteRequest("topic", 1, 55, 15);
        File partition = new File("topic", 1);

        setup(partition);

        DataTransfer dataTransfer = new DataTransfer(new Connection());
        dataTransfer.setMethod(Constants.METHOD.PULL);
        int actual = -1;

        try {
            Method processMethod = DataTransfer.class.getDeclaredMethod("getSegmentNumber", TopicReadWriteRequest.class, File.class, int.class, boolean.class);
            processMethod.setAccessible(true);
            actual = (int) processMethod.invoke(dataTransfer, request, partition, request.getFromOffset(), true);

        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException exception) {
            System.err.println(exception.getMessage());
        }

        Assertions.assertEquals(1, actual);
    }

    /**
     * Create 2 segments with pre-initialized offsets.
     */
    private void setup(File partition) {
        Segment segment1 = new Segment("xyz", 0, 27);
        segment1.addOffset(0);
        segment1.addOffset(5);
        segment1.addOffset(15);
        segment1.addOffset(17);
        segment1.addOffset(21);

        Segment segment2 = new Segment("abc", 1, 59);
        segment2.addOffset(27);
        segment2.addOffset(35);
        segment2.addOffset(42);
        segment2.addOffset(54);

        List<Segment> segments = new ArrayList<>();
        segments.add(segment1);
        segments.add(segment2);

        try {
            Field outputStream = File.class.getDeclaredField("segments");
            outputStream.setAccessible(true);
            outputStream.set(partition, segments);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        try {
            Field outputStream = File.class.getDeclaredField("segment");
            outputStream.setAccessible(true);
            outputStream.set(partition, new Segment("qwe", 2, 87));
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        try {
            Field outputStream = File.class.getDeclaredField("availableSize");
            outputStream.setAccessible(true);
            outputStream.set(partition, 59);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }
    }
}
