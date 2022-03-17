package controllers;

import configuration.Constants;
import configurations.Config;
import models.Header;
import models.Topic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.AppPacketHandler;
import utilities.NodeTimer;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

public class Processor {
    private static final Logger logger = LogManager.getLogger(Processor.class);

    public void process(Config config) {
        if (config.isCreateTopic()) {
            createTopics(config);
        }
    }

    private void createTopics(Config config) {
        //Connect to the load balancer

        try {
            Socket socket = new Socket(config.getLoadBalancer().getAddress(), config.getLoadBalancer().getPort());
            logger.info(String.format("[%s:%d] Successfully connected to the load balancer.", config.getLoadBalancer().getAddress(), config.getLoadBalancer().getPort()));

            Connection connection = new Connection(socket, config.getLoadBalancer().getAddress(), config.getLoadBalancer().getPort());
            if (connection.openConnection()) {
                List<Topic> topics = config.getTopics();
                NodeTimer timer = new NodeTimer();
                int seqNum = 0; //Sequence number of the packet which is indented to send to another host

                for (Topic topic : topics) {
                    logger.info(String.format("[%s:%d] Creating %d partitions of topic %s.", config.getLoadBalancer().getAddress(), config.getLoadBalancer().getPort(), topic.getNumOfPartitions(), topic.getName()));

                    byte[] packet = AppPacketHandler.createAddTopicPacket(topic, seqNum);

                    connection.send(packet);
                    timer.startTimer("TOPIC", Constants.RTT);
                    boolean reading = true;

                    while (reading) {
                        if (timer.isTimeout()) {
                            logger.warn(String.format("[%s:%d] Timeout happen for the packet having topic %s - partition count %d information. Sending again", config.getLoadBalancer().getAddress(), config.getLoadBalancer().getPort(), topic.getName(), topic.getNumOfPartitions()));
                            connection.send(packet);
                            timer.stopTimer();
                            timer.startTimer("TOPIC", Constants.RTT);
                        } else if (connection.isAvailable()) {
                            byte[] responseInBytes = connection.receive();

                            if (responseInBytes != null) {
                                Header.Content header = AppPacketHandler.getHeader(responseInBytes);

                                if (header != null) {
                                    if (header.getType() == Constants.TYPE.ACK.getValue()) {
                                        if (header.getSeqNum() == seqNum) {
                                            logger.info(String.format("[%s:%d] Received an acknowledgment for the packet with the sequence number %d.", config.getLoadBalancer().getAddress(), config.getLoadBalancer().getPort(), seqNum));
                                            reading = false;
                                            seqNum++;
                                            timer.stopTimer();
                                        } else if (header.getSeqNum() < seqNum) {
                                            logger.info(String.format("[%s:%d] Received an acknowledgment for the older packet with the sequence number %d. Ignoring it.", config.getLoadBalancer().getAddress(), config.getLoadBalancer().getPort(), header.getSeqNum()));
                                        }
                                    } else if (header.getType() == Constants.TYPE.NACK.getValue()) {
                                        logger.info(String.format("[%s:%d] Received a negative acknowledgment for the packet having topic %s - partition count %d information.", config.getLoadBalancer().getAddress(), config.getLoadBalancer().getPort(), topic.getName(), topic.getNumOfPartitions()));
                                        reading = false;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (IOException exception) {
            logger.error(String.format("[%s:%d] Fail to make connection with the load balancer.", config.getLoadBalancer().getAddress(), config.getLoadBalancer().getPort()), exception);
        }
    }
}
