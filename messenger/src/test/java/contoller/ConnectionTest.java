package contoller;

import controllers.Connection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

/**
 * Responsible for testing functions in Connection class.
 *
 * @author Palak Jain
 */
public class ConnectionTest {
    private static Connection connection;

    @BeforeAll
    public static void init() {
        connection = new Connection();
    }

    /**
     * Tests if after sending the byte array via the output stream then the send method should return true
     */
    @Test
    public void send_writeBytes_returnTrue() {
        byte[] message = "sample".getBytes(StandardCharsets.UTF_8);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

        try {
            Field outputStream = Connection.class.getDeclaredField("outputStream");
            outputStream.setAccessible(true);
            outputStream.set(connection, dataOutputStream);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        Assertions.assertTrue(connection.send(message));
    }

    /**
     * Tests if send method actually write correct data to the output stream
     */
    @Test
    public void send_writeBytes_writesCorrectData() {
        byte[] message = "sample".getBytes(StandardCharsets.UTF_8);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

        try {
            Field outputStream = Connection.class.getDeclaredField("outputStream");
            outputStream.setAccessible(true);
            outputStream.set(connection, dataOutputStream);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        connection.send(message);

        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        DataOutputStream expectedOutputStream = new DataOutputStream(expected);
        try {
            expectedOutputStream.writeInt(message.length);
            expectedOutputStream.write(message);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Assertions.assertEquals(expected.toString(StandardCharsets.UTF_8), byteArrayOutputStream.toString(StandardCharsets.UTF_8));

        try {
            dataOutputStream.close();
            byteArrayOutputStream.close();
            expectedOutputStream.close();
            expected.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Tests if the reception method receives correct message
     */
    @Test
    public void receive_readBytes_returnArray() {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        try {
            dataOutputStream.writeInt("message".getBytes(StandardCharsets.UTF_8).length);
            dataOutputStream.write("message".getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);

        try {
            Field inputStream = Connection.class.getDeclaredField("inputStream");
            inputStream.setAccessible(true);
            inputStream.set(connection, dataInputStream);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        byte[] actual = connection.receive();

        Assertions.assertEquals("message", new String(actual, StandardCharsets.UTF_8));

        try {
            dataOutputStream.close();
            byteArrayOutputStream.close();
            dataInputStream.close();
            byteArrayInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
