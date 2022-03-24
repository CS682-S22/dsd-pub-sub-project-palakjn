package controller;

import configuration.Constants;
import controllers.CacheManager;
import controllers.Connection;
import controllers.RequestHandler;
import models.Header;
import models.Host;
import models.Topic;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import utilities.PacketHandler;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

public class RequestHandlerTest {

    @Test
    public void createTopic_noBroker_sendNack() {
        Topic topic = new Topic("Sample", 2);

        createTopic_isNack(topic);
    }

    @Test
    public void createTopic_existTopic_sendNack() {
        Topic topic = new Topic("Sample", 3);

        try {
            CacheManager.addTopic(topic);
            createTopic_isNack(topic);
        } finally {
            CacheManager.removeTopic(topic);
        }
    }

    @Test
    public void createTopic_newTopic_shouldCreateTopic() {
        Host sampleBroker = new Host("address", 1234);
        Topic topic = new Topic("Sample", 3);

        try {
            CacheManager.addBroker(sampleBroker);
            Connection connection = new Connection();
            mockConnection(connection);

            sendRequest(connection, topic);

            Assertions.assertTrue(CacheManager.isTopicExist("Sample"));
        } finally {
            CacheManager.removeTopic(topic);
            CacheManager.removeBroker(sampleBroker);
        }
    }

    @Test
    public void processBrokerRequest_addBroker_shouldAdd() {
        Host sampleBroker = new Host("address", 1700);

        try {
            Connection connection = new Connection();
            mockConnection(connection);

            sendReqToAddBroker(connection, sampleBroker);

            Assertions.assertTrue(CacheManager.isBrokerExist(sampleBroker));
        } finally {
            CacheManager.removeBroker(sampleBroker);
        }
    }

    @Test
    public void processBrokerRequest_remBroker_shouldRemove() {
        Host sampleBroker = new Host("address", 1700);

        try {
            CacheManager.addBroker(sampleBroker);

            Connection connection = new Connection();
            mockConnection(connection);

            sendReqToRemBroker(connection, sampleBroker);

            Assertions.assertFalse(CacheManager.isBrokerExist(sampleBroker));
        } finally {
            CacheManager.removeBroker(sampleBroker);
        }
    }

    private void createTopic_isNack(Topic topic) {
        Connection connection = new Connection();
        ByteArrayOutputStream byteArrayOutputStream = mockConnection(connection);

        sendRequest(connection, topic);

        byte[] bytesReceived = Arrays.copyOfRange(byteArrayOutputStream.toByteArray(), 4, 10);

        Header.Content header = PacketHandler.getHeader(bytesReceived);

        Assertions.assertEquals(header.getType(), Constants.TYPE.NACK.getValue());
    }

    private void sendRequest(Connection connection, Topic topic) {
        RequestHandler requestHandler = new RequestHandler(connection);

        try {
            Method processMethod = RequestHandler.class.getDeclaredMethod("createTopic", Topic.class);
            processMethod.setAccessible(true);
            processMethod.invoke(requestHandler, topic);

        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException exception) {
            System.err.println(exception.getMessage());
        }
    }

    private void sendReqToAddBroker(Connection connection, Host broker) {
        byte[] packet = PacketHandler.createPacket(Constants.REQUESTER.BROKER, Constants.TYPE.ADD, broker);
        sendReqToAddRemBroker(connection, packet, Constants.TYPE.ADD.getValue());
    }

    private void sendReqToRemBroker(Connection connection, Host broker) {
        byte[] packet = PacketHandler.createPacket(Constants.REQUESTER.BROKER, Constants.TYPE.REM, broker);
        sendReqToAddRemBroker(connection, packet, Constants.TYPE.REM.getValue());
    }

    private void sendReqToAddRemBroker(Connection connection, byte[] packet, int action) {
        RequestHandler requestHandler = new RequestHandler(connection);

        try {
            Method processMethod = RequestHandler.class.getDeclaredMethod("processBrokerRequest", byte[].class, int.class);
            processMethod.setAccessible(true);
            processMethod.invoke(requestHandler, packet, action);

        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException exception) {
            System.err.println(exception.getMessage());
        }
    }

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
