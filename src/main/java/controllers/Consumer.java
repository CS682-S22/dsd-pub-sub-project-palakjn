package controllers;

import configuration.Constants;
import configurations.AppConstants;
import models.Header;
import models.Properties;
import org.apache.logging.log4j.LogManager;
import utilities.AppPacketHandler;
import utilities.Strings;

import java.util.concurrent.*;

public class Consumer extends Client {
    private BlockingQueue<byte[]> queue;
    private ExecutorService threadPool;
    private int offset;
    private AppConstants.METHOD method;

    public Consumer(Properties properties) {
        super(LogManager.getLogger(Consumer.class), properties);

        queue = new LinkedBlockingDeque<>(AppConstants.QUEUE_BUFFER_SIZE);
        this.threadPool = Executors.newFixedThreadPool(AppConstants.THREAD_COUNT);

        String offset = properties.getValue(Constants.PROPERTY_KEY.OFFSET);
        if (!Strings.isNullOrEmpty(offset) && isNumeric(offset)) {
            this.offset = Integer.parseInt(offset);
        }

        String method = properties.getValue(Constants.PROPERTY_KEY.METHOD);
        if (!Strings.isNullOrEmpty(method)) {
            this.method = AppConstants.findMethodByName(method);
        }

        if (this.method == null) {
            this.method = AppConstants.METHOD.PULL; //By default
        }
    }

    public boolean subscribe(String topic, int key) {
        boolean flag = false;
        byte[] lbPacket = AppPacketHandler.createGetBrokerReq(AppConstants.REQUESTER.CONSUMER, topic, key);
        byte[] brokerRequest = null;

        if (method == AppConstants.METHOD.PULL) {
            brokerRequest = AppPacketHandler.createToBrokerRequest(AppConstants.REQUESTER.CONSUMER, AppConstants.TYPE.PULL, topic, key, offset);
        } else { //POST
            brokerRequest = AppPacketHandler.createToBrokerRequest(AppConstants.REQUESTER.CONSUMER, AppConstants.TYPE.ADD, topic, key, offset);
        }

        if ((broker != null ||
                getBroker(lbPacket, topic, key)) && connectToBroker(brokerRequest, method.name())) {
            isConnected = true;

            threadPool.execute(this::receive);
            flag = true;

            logger.info(String.format("[%s] [%s] Successfully subscribed to the topic: %s - Partition %d.", hostName, method.name(), topic, key));
        } else {
            logger.warn(String.format("[%s] [%s:%d] Either broker details not found or not able to connect to the broker. Not able to get the data of the topic %s - Partition %d.", hostName, broker == null ? null : broker.getAddress(), broker == null ? 0 : broker.getPort(), topic, key));
        }

        return flag;
    }

    public byte[] poll(int milliseconds) {
        byte[] data = null;

        try {
            data = queue.poll(milliseconds, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error(String.format("[%s] Unable to get the data from the queue", hostName), e);
        }

        return data;
    }

    public void receive() {
        while (true) {
            byte[] packet = connection.receive();

            if (packet != null) {
                Header.Content header = AppPacketHandler.getHeader(packet);

                if (header != null && header.getType() == AppConstants.TYPE.DATA.getValue()) {
                    byte[] data = AppPacketHandler.getData(packet);

                    if (data != null) {
                        logger.debug(String.format("[%s] [%s:%d] Received the data from the broker.", hostName, connection.getDestinationIPAddress(), connection.getDestinationPort()));

                        try {
                            queue.put(data);
                        } catch (InterruptedException e) {
                            logger.error(String.format("[%s] Fail to add items to the queue", hostName), e);
                        }
                    } else {
                        logger.warn(String.format("[%s] No data found from the received packet. Ignored", hostName));
                    }
                } else {
                    logger.warn(String.format("[%s] Invalid header received from the broker. Ignored", hostName));
                }
            }
        }
    }
}