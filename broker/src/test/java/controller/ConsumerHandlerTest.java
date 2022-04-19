package controller;

import controllers.Connection;
import controllers.consumer.ConsumerHandler;
import models.data.File;
import models.data.Segment;
import models.requests.TopicReadWriteRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Responsible for testing functions in ConsumerHandler class.
 *
 * @author Palak Jain
 */
public class ConsumerHandlerTest {

    /**
     * Test whether getSegmentNumber() returns the segment number if exact given offset exist
     */
    @Test
    public void getSegmentNumber_exactOffset_returnSegment() {
        TopicReadWriteRequest request = new TopicReadWriteRequest("topic", 1, 0, 15);
        File partition = new File("topic", 1);

        setup(partition);

        ConsumerHandler consumerHandler = new ConsumerHandler(null);
        int actual = -1;

        try {
            Method processMethod = ConsumerHandler.class.getDeclaredMethod("getSegmentNumber", TopicReadWriteRequest.class, File.class);
            processMethod.setAccessible(true);
            actual = (int) processMethod.invoke(consumerHandler, request, partition);

        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException exception) {
            System.err.println(exception.getMessage());
        }

        Assertions.assertEquals(0, actual);
    }

    /**
     * Test whether getSegmentNumber return the segment number if the exact offset don't exist but the offset which is less
     * than the given offset exists
     */
    @Test
    public void getSegmentNumber_inRangeOffset_returnSegment() {
        TopicReadWriteRequest request = new TopicReadWriteRequest("topic", 1, 0, 37);
        File partition = new File("topic", 1);

        setup(partition);

        ConsumerHandler consumerHandler = new ConsumerHandler(new Connection());
        int actual = -1;

        try {
            Method processMethod = ConsumerHandler.class.getDeclaredMethod("getSegmentNumber", TopicReadWriteRequest.class, File.class);
            processMethod.setAccessible(true);
            actual = (int) processMethod.invoke(consumerHandler, request, partition);

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
            Field outputStream = File.class.getDeclaredField("availableSize");
            outputStream.setAccessible(true);
            outputStream.set(partition, 59);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }
    }
}
