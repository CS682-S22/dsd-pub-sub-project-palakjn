package controller;

import configuration.Constants;
import controllers.CacheManager;
import controllers.Connection;
import controllers.RequestHandler;
import models.Header;
import models.Host;
import models.Topic;
import models.requests.CreateTopicRequest;
import models.requests.Request;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import utilities.PacketHandler;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * Responsible for testing functions in RequestHandler class
 *
 * @author Palak Jain
 */
public class RequestHandlerTest {

    /**
     * Test if createTopic() send NACK if creating new topic when there are no brokers.
     */
    @Test
    public void createTopic_noBroker_sendNack() {
        Topic topic = new Topic("Sample", 2);

        createTopic_isNack(topic);
    }

    /**
     * Test if createTopic() send NACK if creating a topic which already exists
     */
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

    /**
     * Test if processBrokerRequest() add new broker to the collection
     */
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

    /**
     * Test if processBrokerRequest() remove broker information from the collection
     */
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

    /**
     * Create request, send to the broker and asserts if the response is NACK
     */
    private void createTopic_isNack(Topic topic) {
        Connection connection = new Connection();
        ByteArrayOutputStream byteArrayOutputStream = mockConnection(connection);

        sendRequest(connection, topic);

        byte[] bytesReceived = Arrays.copyOfRange(byteArrayOutputStream.toByteArray(), 4, 10);

        Header.Content header = PacketHandler.getHeader(bytesReceived);

        Assertions.assertEquals(header.getType(), Constants.TYPE.NACK.getValue());
    }

    /**
     * Send the request to create new topic
     */
    private void sendRequest(Connection connection, Topic topic) {
        RequestHandler requestHandler = new RequestHandler(connection);
        CreateTopicRequest createTopicRequest = new CreateTopicRequest(topic.getName(), topic.getNumOfPartitions(), 2);

        try {
            Method processMethod = RequestHandler.class.getDeclaredMethod("createTopic", CreateTopicRequest.class);
            processMethod.setAccessible(true);
            processMethod.invoke(requestHandler, createTopicRequest);

        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException exception) {
            System.err.println(exception.getMessage());
        }
    }

    /**
     * Create and send request to add new broker
     */
    private void sendReqToAddBroker(Connection connection, Host broker) {
        Request<Host> request = new Request<>(Constants.REQUEST_TYPE.ADD, broker);
        byte[] packet = PacketHandler.createPacket(Constants.REQUESTER.BROKER, Constants.TYPE.REQ, request);
        sendReqToAddRemBroker(connection, packet);
    }

    /**
     * Create and send request to remove broker
     */
    private void sendReqToRemBroker(Connection connection, Host broker) {
        Request<Host> request = new Request<>(Constants.REQUEST_TYPE.REM, broker);
        byte[] packet = PacketHandler.createPacket(Constants.REQUESTER.BROKER, Constants.TYPE.REQ, request);
        sendReqToAddRemBroker(connection, packet);
    }

    /**
     * Send add/remove request to broker
     */
    private void sendReqToAddRemBroker(Connection connection, byte[] packet) {
        RequestHandler requestHandler = new RequestHandler(connection);

        try {
            Method processMethod = RequestHandler.class.getDeclaredMethod("processBrokerRequest", byte[].class, String.class);
            processMethod.setAccessible(true);
            processMethod.invoke(requestHandler, packet, Constants.TYPE.REQ.name());

        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException exception) {
            System.err.println(exception.getMessage());
        }
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
