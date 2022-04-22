package controller;

import configuration.Constants;
import configurations.BrokerConstants;
import controllers.Channels;
import controllers.Connection;
import controllers.database.CacheManager;
import controllers.election.Election;
import models.Host;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Field;

public class ElectionTest {

    @BeforeAll
    public static void init() {
        String topic_key = "topic:0";
        CacheManager.setBroker(new Host(5, "localhost", 3000, 3001));
        CacheManager.setStatus(topic_key, BrokerConstants.BROKER_STATE.READY);

        Connection lbConnection = new Connection();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

        try {
            Field outputStream = Connection.class.getDeclaredField("outputStream");
            outputStream.setAccessible(true);
            outputStream.set(lbConnection, dataOutputStream);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        Channels.upsert(null, lbConnection, Constants.CHANNEL_TYPE.LOADBALANCER);
    }

    @Test
    public void start_shouldElectItselfAsLeader() {
        Election election = new Election();
        election.start("topic:0", new Host("localhost", 4000));

        Assertions.assertTrue(CacheManager.isLeader("topic:0", CacheManager.getBrokerInfo()));
    }

    @AfterAll
    public static void cleanup() {
        CacheManager.removeStatus("topic:0");
        CacheManager.setBroker(null);

        Channels.remove(null, Constants.CHANNEL_TYPE.LOADBALANCER);
        CacheManager.removeLeader("topic:0");
    }
}
