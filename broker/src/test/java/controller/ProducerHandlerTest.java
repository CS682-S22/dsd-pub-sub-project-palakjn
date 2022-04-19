package controller;

import configurations.BrokerConstants;
import controllers.Connection;
import controllers.producer.ProducerHandler;
import models.data.File;
import models.data.Segment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import utilities.BrokerPacketHandler;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Responsible for testing functions in ProducerHandler.
 *
 * @author Palak Jain
 */
public class ProducerHandlerTest {

    /**
     * Test whether receive() method append new date to the current segment or not
     */
    @Test
    public void receive_addData_appendToSegment() {
        Connection connection = addDataToChannel();

        File partition = new File("topic", 1);
        Segment segment = setUpSegment(partition);

        callProducer(connection, partition);

        byte[] buffer = segment.getBuffer();

        Assertions.assertEquals(16, buffer.length);
    }

    /**
     * Test whether receive() method writes correct offset to the current segment
     */
    @Test
    public void receive_addData_writeCorrectOffset() {
        Connection connection = addDataToChannel();

        File partition = new File("topic", 1);
        Segment segment = setUpSegment(partition);

        callProducer(connection, partition);

        int offset = segment.getOffset(3);

        Assertions.assertEquals(12, offset);
    }

    /**
     * Add dummy data to the channel for receive() method to read from
     */
    private Connection addDataToChannel() {
        byte[] data = "data".getBytes(StandardCharsets.UTF_8);
        byte[] packet = BrokerPacketHandler.createDataPacket(BrokerConstants.REQUESTER.PRODUCER, data);
        packet = ByteBuffer.allocate(4 + packet.length).putInt(packet.length).put(packet).array();
        Connection connection = new Connection();

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(packet);
        DataInputStream inputStream = new DataInputStream(byteArrayInputStream);

        try {
            Field outputStream = Connection.class.getDeclaredField("inputStream");
            outputStream.setAccessible(true);
            outputStream.set(connection, inputStream);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        return connection;
    }

    /**
     * Create few initialize segments
     */
    private Segment setUpSegment(File partition) {
        Segment segment = new Segment("xyz", 0, 0);

        byte[] data = "data".getBytes(StandardCharsets.UTF_8);
        for (int index = 1; index < 3; index++) {
            data = ByteBuffer.allocate(data.length + 4).put(data).put("data".getBytes(StandardCharsets.UTF_8)).array();
        }

        List<Integer> offsets = new ArrayList<>();
        offsets.add(0);
        offsets.add(4);
        offsets.add(8);

        try {
            Field outputStream = Segment.class.getDeclaredField("buffer");
            outputStream.setAccessible(true);
            outputStream.set(segment, data);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        try {
            Field outputStream = Segment.class.getDeclaredField("offsets");
            outputStream.setAccessible(true);
            outputStream.set(segment, offsets);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        try {
            Field outputStream = File.class.getDeclaredField("segment");
            outputStream.setAccessible(true);
            outputStream.set(partition, segment);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        try {
            Field outputStream = File.class.getDeclaredField("totalSize");
            outputStream.setAccessible(true);
            outputStream.set(partition, 12);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        return segment;
    }

    private void callProducer(Connection connection, File partition) {
        ProducerHandler handler = new ProducerHandler(connection);

        try {
            Method processMethod = ProducerHandler.class.getDeclaredMethod("receive", File.class);
            processMethod.setAccessible(true);
            processMethod.invoke(handler, partition);

        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException exception) {
            System.err.println(exception.getMessage());
        }
    }
}
