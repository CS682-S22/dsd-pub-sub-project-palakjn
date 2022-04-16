package framework;

import configuration.Constants;
import controllers.Client;
import models.Header;
import models.Properties;
import org.apache.logging.log4j.LogManager;
import utilities.NodeTimer;
import utilities.PacketHandler;
import utilities.Strings;

import java.util.concurrent.*;

/**
 * Responsible for consuming logs from broker
 *
 * @author Palak Jain
 */
public class Consumer extends Client {
    private BlockingQueue<byte[]> queue;
    private ExecutorService threadPool;
    private int offset;
    private Constants.METHOD method;
    private NodeTimer timer;
    private String topic;
    private int key;

    public Consumer(Properties properties) {
        super(LogManager.getLogger("consumer"), properties);

        queue = new LinkedBlockingDeque<>(Constants.QUEUE_BUFFER_SIZE);
        this.threadPool = Executors.newFixedThreadPool(Constants.THREAD_COUNT);

        //Get the offset
        String offset = properties.getValue(Constants.PROPERTY_KEY.OFFSET);
        if (!Strings.isNullOrEmpty(offset) && isNumeric(offset)) {
            this.offset = Integer.parseInt(offset);
        }

        //Get the method PULL or POST
        String method = properties.getValue(Constants.PROPERTY_KEY.METHOD);
        if (!Strings.isNullOrEmpty(method)) {
            this.method = Constants.findMethodByName(method);
        }

        if (this.method == null) {
            this.method = Constants.METHOD.PULL; //By default
        }

        timer = new NodeTimer();
    }

    /**
     * Subscribe the topic and partition with the broker.
     */
    public boolean subscribe(String topic, int key) {
        boolean flag = false;
        byte[] lbPacket = PacketHandler.createGetBrokerReq(Constants.REQUESTER.CONSUMER, topic, key);
        byte[] brokerRequest;

        if (method == Constants.METHOD.PULL) {
            brokerRequest = PacketHandler.createToBrokerRequest(Constants.REQUESTER.CONSUMER, Constants.TYPE.PULL, topic, key, offset, Constants.CONSUMER_MAX_PULL_SIZE);
        } else { //POST
            brokerRequest = PacketHandler.createToBrokerRequest(Constants.REQUESTER.CONSUMER, Constants.TYPE.SUB, topic, key, offset);
        }

        //Get the broker and connect to the broker
        if ((broker != null ||
                getBroker(lbPacket, topic, key)) && connectToBroker(brokerRequest)) {
            this.topic = topic;
            this.key = key;
            isConnected = true;
            flag = true;

            logger.info(String.format("[%s] [%s] Successfully subscribed to the topic: %s - Partition %d.", hostName, method.name(), topic, key));

            if (method == Constants.METHOD.PULL) {
                timer.startTimer("CONSUMER PULL TIMER", Constants.CONSUMER_WAIT_TIME);
                threadPool.execute(this::processPULL);
            } else {
                threadPool.execute(this::processPUSH);
            }
        } else {
            logger.warn(String.format("[%s] [%s:%d] Either broker details not found or not able to connect to the broker. Not able to get the data of the topic %s - Partition %d.", hostName, broker == null ? null : broker.getAddress(), broker == null ? 0 : broker.getPort(), topic, key));
        }

        return flag;
    }

    /**
     * Get the data from the queue within the given timeline.
     */
    public byte[] poll(int milliseconds) {
        byte[] data = null;

        try {
            data = queue.poll(milliseconds, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error(String.format("[%s] Unable to get the data from the queue", hostName), e);
        }

        return data;
    }

    /**
     * Try to receive data from broker.
     * Will re-send the pull request if timeout or received requested number of logs.
     */
    private void processPULL() {
        int count = 0;

        while (isConnected && connection.isOpen()) {
            if (timer.isTimeout()) {
                timer.stopTimer();

                logger.info(String.format("[%s] Timeout happen. Received %d number of logs. Sending PULL request to broker to get topic %s:%d information from offset %d", hostName, count, topic, key, offset));
                //Sending the pull request to the broker with new offset
                sendPULLRequest();
                count = 0;
            } else if (count == Constants.CONSUMER_MAX_PULL_SIZE) {
                timer.stopTimer();

                logger.debug(String.format("[%s]  Received %d number of logs. Sending pull request to broker to get topic %s:%d information from offset %d", hostName, count, topic, key, offset));
                //Sending the pull request to the broker with new offset
                sendPULLRequest();
                count = 0;
            } else if (connection.isAvailable()) {
                byte[] data = connection.receive();
                if (processData(data)) {
                    count++;
                }
            }
        }
    }

    /**
     * Receive data from the broker
     */
    private void processPUSH() {
        while (isConnected && connection.isOpen()) {
            byte[] data = connection.receive();

            processData(data);
        }
    }

    /**
     * Send pull request to the broker to get n number of data from the given offset
     */
    private void sendPULLRequest() {
        byte[] request = PacketHandler.createToBrokerRequest(Constants.REQUESTER.CONSUMER, Constants.TYPE.PULL, topic, key, offset, Constants.CONSUMER_MAX_PULL_SIZE);
        if (connectToBroker(request)) {
            timer.startTimer("CONSUMER PULL TIMER", Constants.CONSUMER_WAIT_TIME);
            isConnected = true;
        } else {
            logger.warn(String.format("[%s] Not able to connect to the broker to get topic %s:%d offset: %d information", hostName, topic, key, offset));
            isConnected = false;
        }
    }

    /**
     * Decode the received packet and write to the queue for the application to read from.
     */
    private boolean processData(byte[] packet) {
        boolean flag = false;

        if (packet != null) {
            Header.Content header = PacketHandler.getHeader(packet);

            if (header != null) {
                if (header.getType() == Constants.TYPE.DATA.getValue()) {
                    byte[] data = PacketHandler.getData(packet);

                    if (data != null) {
                        if (method == Constants.METHOD.PULL) {
                            logger.debug(String.format("[%s] [%s:%d] [PULL] Received the data from the broker with the offset as %d.", hostName, connection.getDestinationIPAddress(), connection.getDestinationPort(), header.getOffset()));
                        } else {
                            logger.debug(String.format("[%s] [%s:%d] [PUSH] Received the data from the broker.", hostName, connection.getDestinationIPAddress(), connection.getDestinationPort()));
                        }

                        try {
                            queue.put(data);
                            flag = true;

                            if (method == Constants.METHOD.PULL) {
                                offset = header.getOffset();
                            }
                        } catch (InterruptedException e) {
                            logger.error(String.format("[%s] [%s] Fail to add item to the queue", hostName, method.name()), e);
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

        return flag;
    }
}
