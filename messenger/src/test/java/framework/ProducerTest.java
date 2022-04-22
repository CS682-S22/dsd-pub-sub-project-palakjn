package framework;

import controllers.Client;
import controllers.Connection;
import models.Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Responsible for testing functions in producer class.
 *
 * @author Palak Jain
 */
public class ProducerTest {

    /**
     * Test if send method should send item if there are items in a queue
     */
    @Test
    public void send_withItemInQueue_ShouldSend() {
        byte[] data = "sample".getBytes(StandardCharsets.UTF_8);

        BlockingQueue<byte[]> queue = new LinkedBlockingDeque<>();
        queue.add(data);

        Producer producer = new Producer(new Properties());

        try {
            Field outputStream = Producer.class.getDeclaredField("queue");
            outputStream.setAccessible(true);
            outputStream.set(producer, queue);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        Connection connection = new Connection();

        ByteArrayOutputStream byteArrayOutputStream = mockConnection(connection);

        try {
            Field outputStream = Client.class.getDeclaredField("connection");
            outputStream.setAccessible(true);
            outputStream.set(producer, connection);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        try {
            Field outputStream = Client.class.getDeclaredField("isConnected");
            outputStream.setAccessible(true);
            outputStream.set(producer, true);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        //Creating separate thread which will listen for data and add to the queue
        Thread thread = new Thread(() -> {
            try {
                Method processMethod = Producer.class.getDeclaredMethod("send", String.class, int.class);
                processMethod.setAccessible(true);
                processMethod.invoke(producer, "topic", 0);

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

        byte[] expected = Arrays.copyOfRange(byteArrayOutputStream.toByteArray(), 12, byteArrayOutputStream.size());
        Assertions.assertEquals(new String(data), new String(expected));
    }

    /**
     * Add outputstream for the connection object to write to
     */
    private ByteArrayOutputStream mockConnection(Connection connection) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

        try {
            Field outputStream = Connection.class.getDeclaredField("outputStream");
            outputStream.setAccessible(true);
            outputStream.set(connection, dataOutputStream);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        return byteArrayOutputStream;
    }
}
