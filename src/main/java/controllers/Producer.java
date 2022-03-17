package controllers;

import configuration.Constants;
import configurations.AppConstants;
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
    private volatile boolean running = true;

    public Producer(Properties properties) {
        super(LogManager.getLogger(Producer.class), properties);

        queue = new LinkedBlockingDeque<>(AppConstants.QUEUE_BUFFER_SIZE);
        this.threadPool = Executors.newFixedThreadPool(AppConstants.THREAD_COUNT);
    }

    public void send(String topic, int key, byte[] data) {
        if (!isConnected) {
            byte[] lbPacket = AppPacketHandler.createGetBrokerReq(AppConstants.REQUESTER.PRODUCER, topic, key);
            byte[] brokerRequest = AppPacketHandler.createToBrokerRequest(AppConstants.REQUESTER.PRODUCER, AppConstants.TYPE.ADD, topic, key);

            if ((broker != null ||
                    getBroker(lbPacket, topic, key)) && connectToBroker(brokerRequest, AppConstants.TYPE.ADD.name())) {
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
}
