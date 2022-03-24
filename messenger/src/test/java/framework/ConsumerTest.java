package framework;

import configuration.Constants;
import controllers.Client;
import controllers.Connection;
import models.Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import utilities.PacketHandler;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Responsible for testing functions in Consumer class
 *
 * @author Palak Jain
 */
public class ConsumerTest {

    /**
     * Test if poll method should return item if there are items in a queue
     */
    @Test
    public void poll_withItemInQueue_shouldReturnItem() {
        byte[] sample = "sample".getBytes(StandardCharsets.UTF_8);

        BlockingQueue<byte[]> queue = new LinkedBlockingDeque<>();
        queue.add(sample);

        Consumer consumer = new Consumer(new Properties());

        try {
            Field outputStream = Consumer.class.getDeclaredField("queue");
            outputStream.setAccessible(true);
            outputStream.set(consumer, queue);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        byte[] actual = consumer.poll(1000);

        Assertions.assertEquals(sample, actual);
    }

    /**
     * Test if poll method should return item if processPush method has added an item to the queue
     */
    @Test
    public void processPUSH_getData_pollShouldReturnItem() {
        method_getData_pollShouldReturnItem("processPUSH");
    }

    /**
     * Test if poll method should return item if processPULL method has added an item to the queue
     */
    @Test
    public void processPULL_getData_pollShouldReturnItem() {
        method_getData_pollShouldReturnItem("processPULL");
    }

    /**
     * Assert equal if poll method should return item if the given method has added an item to the queue
     */
    private void method_getData_pollShouldReturnItem(String methodName) {
        byte[] expected = "sample".getBytes(StandardCharsets.UTF_8);

        Connection connection = addDataToChannel();

        Consumer consumer = new Consumer(new Properties());

        try {
            Field outputStream = Client.class.getDeclaredField("connection");
            outputStream.setAccessible(true);
            outputStream.set(consumer, connection);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        try {
            Field outputStream = Client.class.getDeclaredField("isConnected");
            outputStream.setAccessible(true);
            outputStream.set(consumer, true);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        //Creating separate thread which will listen for data and add to the queue
        Thread thread = new Thread(() -> {
            try {
                Method processMethod = Consumer.class.getDeclaredMethod(methodName);
                processMethod.setAccessible(true);
                processMethod.invoke(consumer);

            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException exception) {
                System.err.println(exception.getMessage());
            }
        });

        thread.start();

        //Sleeping for 300 milliseconds before closing the connection. Allowing thread to complete one loop
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        connection.closeConnection();

        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        byte[] actual = consumer.poll(1000);

        Assertions.assertEquals(new String(expected), new String(actual));
    }

    /**
     * Add dummy data to the channel for receive() method to read from
     */
    private Connection addDataToChannel() {
        byte[] data = "sample".getBytes(StandardCharsets.UTF_8);
        byte[] packet = PacketHandler.createDataPacket(Constants.REQUESTER.BROKER, data);
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
}
