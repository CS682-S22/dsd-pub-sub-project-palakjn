package framework;

import configuration.Constants;
import controllers.Client;
import models.Header;
import models.Host;
import models.Properties;
import org.apache.logging.log4j.LogManager;
import utilities.PacketHandler;

import java.util.concurrent.*;

/**
 * Responsible for producing logs to the broker.
 *
 * @author Palak Jain
 */
public class Producer extends Client {
    private BlockingQueue<byte[]> queue;
    private ExecutorService threadPool;

    public Producer(Properties properties) {
        super(LogManager.getLogger("producer"), properties);

        queue = new LinkedBlockingDeque<>(Constants.QUEUE_BUFFER_SIZE);
        this.threadPool = Executors.newFixedThreadPool(Constants.THREAD_COUNT);
    }

    /**
     * Send the data to the broker which is holding the given topic and key
     */
    public void send(String topic, int key, byte[] data) {
        if (!isConnected) {
            if (getBrokerAndConnect(topic, key)) {
                isConnected = true;

                threadPool.execute(() -> send(topic, key));
            } else {
                logger.warn(String.format("[%s:%d] [%s] Either broker details not found or not able to connect to the broker. Not able to send the data of the topic %s - Partition %d.", broker == null ? null : broker.getAddress(), broker == null ? 0 : broker.getPort(), hostName, topic, key));
                return;
            }
        }

        try {
            queue.put(data);
        } catch (InterruptedException e) {
            logger.error(String.format("[%s] Unable to add an item to the queue", hostName), e);
        }
    }

    /**
     * Get the data from the queue and send to the broker in order.
     */
    private void send(String topic, int key) {
        int seqNum = 0;

        while (isConnected && connection.isOpen()) {
            try {
                boolean running = true;
                boolean retry;
                byte[] data = queue.poll(Constants.PRODUCER_WAIT_TIME, TimeUnit.MILLISECONDS);

                if (data != null) {
                    byte[] dataPacket = PacketHandler.createDataPacket(Constants.REQUESTER.PRODUCER, data, seqNum);

                    while (running && connection.isOpen()) {
                        connection.send(dataPacket);
                        connection.setTimer(Constants.ACK_WAIT_TIME);
                        //Waiting for an acknowledgment
                        byte[] response = connection.receive();

                        if (response != null) {
                            //Received the response in a timeout period.
                            Header.Content header = PacketHandler.getHeader(response);

                            if (header.getType() == Constants.TYPE.ACK.getValue() && header.getSeqNum() == seqNum) {
                                //Received ACK of the packet
                                logger.info(String.format("[%s:%d] [%s] Send %d number of bytes to the broker with the seq num %d.", broker.getAddress(), broker.getPort(), hostName, data.length, seqNum));
                                seqNum++;
                                running = false;
                                retry = false;
                            } else if (header.getType() == Constants.TYPE.NACK.getValue() && header.getSeqNum() == seqNum) {
                                //Received NACK response for the packet
                                logger.info(String.format("[%s:%d] [%s] Received NACK response from the broker for the packet with %d with sequence number. Sleeping for %d time before retrying", broker.getAddress(), broker.getPort(), hostName, seqNum, Constants.PRODUCER_SLEEP_TIME));
                                Thread.sleep(Constants.PRODUCER_SLEEP_TIME);
                                retry = true;
                            } else {
                                logger.warn(String.format("[%s:%d] [%s] Received invalid response %s from the broker with the seqNum as %d. Expecting seqNum: %d. Retrying", broker.getAddress(), broker.getPort(), hostName, header.getType(), header.getSeqNum(), seqNum));
                                retry = true;
                            }
                        } else {
                            //Timeout. Retry
                            logger.warn(String.format("[%s:%d] [%s] Timeout. Received no response from the broker. Retrying", broker.getAddress(), broker.getPort(), hostName));
                            retry = true;
                        }

                        if (retry) {
                            connection.resetTimer();

                            Host oldBroker = new Host(broker);
                            if (getBrokerAndConnect(topic, key)) {
                                if (!oldBroker.equals(broker)) {
                                    logger.info(String.format("[%s] Received new leader information from broker. Reset sequence number to 0", hostName));
                                    seqNum = 0;
                                }
                            } else {
                                logger.warn(String.format("[%s] Fail to get broker info from load balancer or during connection establishment with broker. Not retrying", hostName));
                                running = false;
                            }
                        }
                    }
                }
            } catch (InterruptedException e) {
                logger.error(String.format("[%s] Interrupted while getting data to post to the broker", hostName), e);
            }
        }
    }

    /**
     * Get broker information from load balancer and connect to it.
     */
    private boolean getBrokerAndConnect(String topic, int key) {
        byte[] lbPacket = PacketHandler.createGetBrokerReq(Constants.REQUESTER.PRODUCER, topic, key);
        byte[] brokerRequest = PacketHandler.createToBrokerRequest(Constants.REQUESTER.PRODUCER, Constants.TYPE.REQ, topic, key);

        return (broker != null ||
                getBroker(lbPacket, topic, key)) && connectToBroker(brokerRequest);
    }
}
