package controllers;

import configuration.Constants;
import models.Partition;
import models.Properties;
import org.apache.logging.log4j.LogManager;
import utilities.AppPacketHandler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

public class Producer extends Client {
    private BlockingQueue<byte[]> queue;
    private ExecutorService threadPool;
    private boolean running = true;

    public Producer(Properties properties) {
        super(LogManager.getLogger(Producer.class), properties);

        queue = new LinkedBlockingDeque<>(100);
        this.threadPool = Executors.newFixedThreadPool(1);
    }

    public void send(String topic, int key, byte[] data) {
        if (!isConnected) {
            byte[] packet = AppPacketHandler.createToBrokerRequest(topic, key);
            if ((broker != null ||
                    getBroker(topic, key)) && connectToBroker(packet, Constants.TYPE.ADD.name())) {
                isConnected = true;

                threadPool.execute(this::send);
            } else {
                logger.warn(String.format("[%s:%d] Either broker details not found or not able to connect to the broker. Not able to send the data of the topic %s - Partition %d.", broker == null ? null : broker.getAddress(), broker == null ? 0 : broker.getPort(), topic, key));
                return;
            }
        }

        byte[] dataPacket = AppPacketHandler.createDataPacket(data);
        try {
            queue.put(dataPacket);
        } catch (InterruptedException e) {
            logger.error("Unable to add an item to the queue", e);
        }
    }

    private void send() {
        while (running) {
            try {
                byte[] data = queue.take();
                if (connection.send(data)) {
                    logger.info(String.format("[%s:%d] Send the data to the broker", broker.getAddress(), broker.getPort()));
                } else {
                    logger.warn(String.format("[%s:%d] Not able to send the data to the broker", broker.getAddress(), broker.getPort()));
                }
            } catch (InterruptedException e) {
                logger.error("Interrupted while getting data to post to the broker", e);
            }
        }
    }

    private boolean getBroker(String topic, int partitionNum) {
        boolean isSuccess = false;

        byte[] packet = AppPacketHandler.createGetBrokerReq(topic, partitionNum);
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
