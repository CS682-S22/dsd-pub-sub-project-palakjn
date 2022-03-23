package framework;

import configuration.Constants;
import controllers.Client;
import models.Properties;
import org.apache.logging.log4j.LogManager;
import utilities.PacketHandler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

public class Producer extends Client {
    private BlockingQueue<byte[]> queue;
    private ExecutorService threadPool;
    private volatile boolean running = true;

    public Producer(Properties properties) {
        super(LogManager.getLogger("producer"), properties);

        queue = new LinkedBlockingDeque<>(Constants.QUEUE_BUFFER_SIZE);
        this.threadPool = Executors.newFixedThreadPool(Constants.THREAD_COUNT);
    }

    public void send(String topic, int key, byte[] data) {
        if (!isConnected) {
            byte[] lbPacket = PacketHandler.createGetBrokerReq(Constants.REQUESTER.PRODUCER, topic, key);
            byte[] brokerRequest = PacketHandler.createToBrokerRequest(Constants.REQUESTER.PRODUCER, Constants.TYPE.ADD, topic, key);

            if ((broker != null ||
                    getBroker(lbPacket, topic, key)) && connectToBroker(brokerRequest, Constants.TYPE.ADD.name())) {
                isConnected = true;

                threadPool.execute(this::send);
            } else {
                logger.warn(String.format("[%s:%d] [%s] Either broker details not found or not able to connect to the broker. Not able to send the data of the topic %s - Partition %d.", broker == null ? null : broker.getAddress(), broker == null ? 0 : broker.getPort(), hostName, topic, key));
                return;
            }
        }

        byte[] dataPacket = PacketHandler.createDataPacket(data);
        try {
            queue.put(dataPacket);
        } catch (InterruptedException e) {
            logger.error(String.format("[%s] Unable to add an item to the queue", hostName), e);
        }
    }

    private void send() {
        while (running) {
            try {
                byte[] data = queue.take();

                if (connection.send(data)) {
                    logger.warn(String.format("[%s:%d] [%s] Send %d number of bytes to the broker", broker.getAddress(), broker.getPort(), hostName, data.length));
                } else {
                    logger.warn(String.format("[%s:%d] [%s] Not able to send the data to the broker", broker.getAddress(), broker.getPort(), hostName));
                }
            } catch (InterruptedException e) {
                logger.error(String.format("[%s] Interrupted while getting data to post to the broker", hostName), e);
            }
        }
    }
}
