package controller;

import controllers.Broker;
import controllers.Brokers;
import controllers.database.CacheManager;
import controllers.loadBalancer.LBHandler;
import models.Host;
import models.Partition;
import models.Topic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Responsible for testing functions inside LBHandlerTest
 *
 * @author Palak Jain
 */
public class LBHandlerTest {

    @BeforeAll
    public static void init() {
        CacheManager.setBroker(new Host("localhost", 8000));

        String topicKey = "topic:0";

        Broker broker1 = new Broker("localhost:3000");
        Broker broker2 = new Broker("localhost:3001");
        Broker broker3 = new Broker("localhost:3002");
        Broker broker4 = new Broker("localhost:3003");

        CacheManager.setLeader(topicKey, broker1);
        CacheManager.addBroker(topicKey, broker2);
        CacheManager.addBroker(topicKey, broker3);

        List<Host> updatedBrokers = new ArrayList<>();
        updatedBrokers.add(broker4);
        updatedBrokers.add(broker2);

        Partition partition = new Partition("topic", 0, broker1, updatedBrokers);
        Topic topic = new Topic("topic", partition);

        LBHandler lbHandler = new LBHandler();

        try {
            Method processMethod = LBHandler.class.getDeclaredMethod("updateTopic", Topic.class);
            processMethod.setAccessible(true);
            processMethod.invoke(lbHandler, topic);

        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException exception) {
            System.err.println(exception.getMessage());
        }
    }

    @Test
    public void updateTopic_updateMembership_shouldReflectNewMember() {
        String topicKey = "topic:0";
        Broker broker4 = new Broker("localhost:3003");

        Brokers brokers = CacheManager.getBrokers(topicKey);

        Assertions.assertTrue(brokers.contains(broker4));
    }

    @Test
    public void updateTopic_updateMembership_shouldReflectRemovalOfOldMember() {
        String topicKey = "topic:0";
        Broker broker3 = new Broker("localhost:3002");

        Brokers brokers = CacheManager.getBrokers(topicKey);

        Assertions.assertFalse(brokers.contains(broker3));
    }

    @AfterAll
    public static void cleanUp() {
        String topicKey = "topic:0";
        Broker broker1 = new Broker("localhost:3000");
        Broker broker2 = new Broker("localhost:3001");
        Broker broker3 = new Broker("localhost:3002");
        Broker broker4 = new Broker("localhost:3003");

        CacheManager.setBroker(null);
        CacheManager.removeBroker(topicKey, broker1);
        CacheManager.removeBroker(topicKey, broker2);
        CacheManager.removeBroker(topicKey, broker3);
        CacheManager.removeBroker(topicKey, broker4);

        CacheManager.removeLeader(topicKey);
    }
}
