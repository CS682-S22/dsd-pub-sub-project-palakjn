package controllers;

import configuration.Constants;
import models.Partition;
import models.Properties;
import org.apache.logging.log4j.LogManager;
import utilities.ProducerPacketHandler;

public class Producer extends Client {

    public Producer(Properties properties) {
        super(LogManager.getLogger(Producer.class), properties);
    }

    public boolean send(String topic, int key, byte[] data) {
        if (!isConnected) {
            byte[] packet = ProducerPacketHandler.createRequest(topic, key);
            if ((broker != null ||
                getBroker(topic, key)) && connectToBroker(packet, Constants.TYPE.REQ.name())) {
                isConnected = true;
            } else {
                logger.warn(String.format("[%s:%d] Either broker details not found or not able to connect to the broker. Not able to send the data of the topic %s - Partition %d.", broker == null ? null : broker.getAddress(), broker == null ? 0 : broker.getPort(), topic, key));
                return false;
            }
        }

        byte[] dataPacket = ProducerPacketHandler.createDataPacket(data);
        if (connection.send(dataPacket)) {
            logger.info(String.format("[%s:%d] Send the data of the topic %s - Partition %d to the broker", broker.getAddress(), broker.getPort(), topic, key));
            return true;
        } else {
            logger.warn(String.format("[%s:%d] Not able to send the data of the topic %s - Partition %d to the broker", broker.getAddress(), broker.getPort(), topic, key));
            return false;
        }
    }

    public void close() {
        //Sending FIN packet
        byte[] packet = ProducerPacketHandler.createFINPacket();
        connection.send(packet);

        //Close connection
        connection.closeConnection();
        isConnected = false;
    }

    private boolean getBroker(String topic, int partitionNum) {
        boolean isSuccess = false;

        byte[] packet = ProducerPacketHandler.createRequest(topic, partitionNum);
        Partition partition = getBroker(packet);

        if (partition != null && partition.getBroker() != null && partition.getBroker().isValid()) {
            broker = partition.getBroker();
            isSuccess = true;
            logger.info(String.format("Received broker information: %s:%d which is holding the information of topic %s - partition %d.", broker.getAddress(), broker.getPort(), topic, partitionNum));
        } else {
            logger.warn(String.format("No broker information found which is holding the information of topic %s - partition %d.",  topic, partitionNum));
        }

        return isSuccess;
    }
}
