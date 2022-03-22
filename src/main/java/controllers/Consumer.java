package controllers;

import configuration.Constants;
import configurations.AppConstants;
import models.Header;
import models.Properties;
import org.apache.logging.log4j.LogManager;
import utilities.AppPacketHandler;
import utilities.NodeTimer;
import utilities.Strings;

import java.util.concurrent.*;

public class Consumer extends Client {
    private BlockingQueue<byte[]> queue;
    private ExecutorService threadPool;
    private int offset;
    private AppConstants.METHOD method;
    private NodeTimer timer;
    private String topic;
    private int key;

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

        timer = new NodeTimer();
    }

    public boolean subscribe(String topic, int key) {
        boolean flag = false;
        byte[] lbPacket = AppPacketHandler.createGetBrokerReq(AppConstants.REQUESTER.CONSUMER, topic, key);
        byte[] brokerRequest;

        if (method == AppConstants.METHOD.PULL) {
            brokerRequest = AppPacketHandler.createToBrokerRequest(AppConstants.REQUESTER.CONSUMER, AppConstants.TYPE.PULL, topic, key, offset);
        } else { //POST
            brokerRequest = AppPacketHandler.createToBrokerRequest(AppConstants.REQUESTER.CONSUMER, AppConstants.TYPE.ADD, topic, key, offset);
        }

        if ((broker != null ||
                getBroker(lbPacket, topic, key)) && connectToBroker(brokerRequest, method.name())) {
            this.topic = topic;
            this.key = key;
            isConnected = true;
            flag = true;

            logger.info(String.format("[%s] [%s] Successfully subscribed to the topic: %s - Partition %d.", hostName, method.name(), topic, key));

            if (method == AppConstants.METHOD.PULL) {
                timer.startTimer("CONSUMER PULL TIMER", AppConstants.CONSUMER_WAIT_TIME);
                threadPool.execute(this::processPULL);
            } else {
                threadPool.execute(this::processPUSH);
            }
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

    private void processPULL() {
        while (isConnected && connection.isOpen()) {
            if (timer.isTimeout()) {
                logger.debug(String.format("[%s] Timeout happen. Will try to receive data for %d amount of time else will re-send the packet.", hostName, AppConstants.CONSUMER_WAIT_TIME));
                timer.stopTimer();

                //Putting timeout period on socket input stream read method
                connection.setTimeOut(AppConstants.CONSUMER_WAIT_TIME);

                byte[] data = connection.receive();
                while (data != null) {
                    logger.debug("Received data after time-out too");
                    processData(data);

                    data = connection.receive();
                }

                //First, closing the connection
                connection.closeConnection();

                //Sending the pull request to the broker with new offset
                logger.info(String.format("[%s] Sending PULL request to broker to get topic %s:%d information from offset %d", hostName, topic, key, offset));
                byte[] request = AppPacketHandler.createToBrokerRequest(AppConstants.REQUESTER.CONSUMER, AppConstants.TYPE.PULL, topic, key, offset);
                if (connectToBroker(request, AppConstants.TYPE.PULL.name())) {
                    timer.startTimer("CONSUMER PULL TIMER", AppConstants.CONSUMER_WAIT_TIME);
                } else {
                    logger.warn(String.format("[%s] Not able to connect to the broker to get topic %s:%d offset: %d information", hostName, topic, key, offset));
                    isConnected = true;
                }
            } else if (connection.isAvailable()) {
                byte[] data = connection.receive();
                processData(data);
            }
        }
    }

    private void processPUSH() {
        while (isConnected && connection.isOpen()) {
            byte[] data = connection.receive();

            processData(data);
        }
    }

    private void processData(byte[] packet) {
        if (packet != null) {
            Header.Content header = AppPacketHandler.getHeader(packet);

            if (header != null) {
                if (header.getType() == AppConstants.TYPE.DATA.getValue()) {
                    byte[] data = AppPacketHandler.getData(packet);

                    if (data != null) {
                        if (method == AppConstants.METHOD.PULL) {
                            logger.debug(String.format("[%s] [%s:%d] [PULL] Received the data from the broker with the offset as %d.", hostName, connection.getDestinationIPAddress(), connection.getDestinationPort(), header.getOffset()));
                        } else {
                            logger.debug(String.format("[%s] [%s:%d] [PUSH] Received the data from the broker.", hostName, connection.getDestinationIPAddress(), connection.getDestinationPort()));
                        }

                        try {
                            queue.put(data);

                            if (method == AppConstants.METHOD.PULL) {
                                offset = header.getOffset();
                            }
                        } catch (InterruptedException e) {
                            logger.error(String.format("[%s] [%s] Fail to add items to the queue", hostName, method.name()), e);
                        }
                    } else {
                        logger.warn(String.format("[%s] [%s] No data found from the received packet. Ignored", hostName, method.name()));
                    }
                } else if (header.getType() == Constants.TYPE.NACK.getValue()) {
                    logger.warn(String.format("[%s] [%s] Offset %d not exist in the broker . Ignored", hostName, method.name(), offset));
            } else
                logger.warn(String.format("[%s] [%s] Invalid header received from the broker. Ignored", hostName, method.name()));
            }
        }
    }
}
