package controller;

import configuration.Constants;
import configurations.BrokerConstants;
import controllers.Connection;
import controllers.broker.BrokerHandler;
import controllers.database.CacheManager;
import models.Header;
import models.Host;
import models.data.File;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import utilities.BrokerPacketHandler;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.util.Arrays;

/**
 * Test the functions inside Broker Handler class
 *
 * @author Palak Jain
 */
public class BrokerHandlerTest {

    @BeforeAll
    public static void init() {
        String topic_key = "topic:0";
        CacheManager.setBroker(new Host(5, "localhost", 3000, 3001));
        CacheManager.setStatus(topic_key, BrokerConstants.BROKER_STATE.READY);

        File partition = new File("topic", 0);
        CacheManager.addPartition("topic", 0 , partition);
    }

    /**
     * Test if after sending Offset request, broker getting correct and expected offset
     */
    @Test
    public void processREQRequest_shouldReturnCorrectOffset() {
        File partition = CacheManager.getPartition("topic", 0);

        try {
            Field outputStream = File.class.getDeclaredField("offset");
            outputStream.setAccessible(true);
            outputStream.set(partition, 5);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        try {
            Field outputStream = File.class.getDeclaredField("totalSize");
            outputStream.setAccessible(true);
            outputStream.set(partition, 15);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        Connection connection = new Connection();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

        try {
            Field outputStream = Connection.class.getDeclaredField("outputStream");
            outputStream.setAccessible(true);
            outputStream.set(connection, dataOutputStream);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        byte[] packet = BrokerPacketHandler.createOffsetRequest("topic:0");
        Header.Content header = Header.Content.newBuilder().setRequester(Constants.REQUESTER.BROKER.getValue()).setType(Constants.TYPE.REQ.getValue()).build();

        BrokerHandler brokerHandler = new BrokerHandler(connection);
        brokerHandler.handleRequest(header, packet);

        byte[] receivedBytes = byteArrayOutputStream.toByteArray();
        receivedBytes = Arrays.copyOfRange(receivedBytes, 4, receivedBytes.length);

        byte[] data = BrokerPacketHandler.getData(receivedBytes);

        if (data != null) {
            int offset = getOffset(data);

            Assertions.assertEquals(5, offset);
        }
    }

   private int getOffset(byte[] data) {
        int offset = 0;

        String json = new String(data);

        if (json.contains("\"offset\":")) {
            int indexOf = json.indexOf("\"offset\":");
            char nextChar = json.charAt(indexOf + 9);

            offset = Integer.parseInt(String.valueOf(nextChar));
        }

        return offset;
   }

    @AfterAll
    public static void cleanup() {
        CacheManager.removeStatus("topic:0");
        CacheManager.setBroker(null);
        CacheManager.removePartition("topic:0");
    }
}
