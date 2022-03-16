package controllers;

import configuration.Constants;
import models.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.JSONDesrializer;
import utilities.LBPacketHandler;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RequestHandler {
    private static final Logger logger = LogManager.getLogger(RequestHandler.class);
    private HostService hostService;
    private Connection connection;
    private int curSeq = 0;

    public RequestHandler(Connection connection) {
        this.connection = connection;
        this.hostService = new HostService(connection, logger);
    }

    public void process() {
        boolean running = true;

        while (running) {
            byte[] request = connection.receive();

            if (request != null) {
                Header.Content header = LBPacketHandler.getHeader(request);

                if (header.getRequester() == Constants.REQUESTER.BROKER.getValue()) {
                    if (header.getSeqNum() == curSeq) {
                        logger.info(String.format("[%s:%d] Received request from broker with sequence number: %d", connection.getDestinationIPAddress(), connection.getDestinationPort(), curSeq));
                        processBrokerRequest(request, header.getType());
                    } else if ((curSeq == 0 && header.getSeqNum() == 1) || (curSeq == 1 && header.getSeqNum() == 0)) {
                        logger.info(String.format("[%s:%d] Received another same request from broker with sequence number: %d. Sending acknowledgement.", connection.getDestinationIPAddress(), connection.getDestinationPort(), curSeq));
                        hostService.sendACK(Constants.REQUESTER.LOAD_BALANCER, header.getSeqNum());
                    }
                } else if ((header.getRequester() == Constants.REQUESTER.PRODUCER.getValue() ||
                        header.getRequester() == Constants.REQUESTER.CONSUMER.getValue()) &&
                        header.getType() == Constants.TYPE.REQ.getValue()) {
                    logger.info(String.format("[%s:%d] Received request from %s to get broker details for a partition/topic.", connection.getDestinationIPAddress(), connection.getDestinationPort(), Constants.REQUESTER.values()[header.getRequester()].name()));
                    sendBrokerDetails(request);
                } else if (header.getRequester() == Constants.REQUESTER.TOPIC.getValue()) {
                    if (header.getSeqNum() == curSeq) {
                        logger.info(String.format("[%s:%d] Received request from host with sequence number: %d to create the topic", connection.getDestinationIPAddress(), connection.getDestinationPort(), curSeq));
                        processTopicRequest(request, header.getType());
                    } else if ((curSeq == 0 && header.getSeqNum() == 1) || (curSeq == 1 && header.getSeqNum() == 0)) {
                        logger.info(String.format("[%s:%d] Received another same request from host with sequence number: %d to create the topic. Sending an acknowledgement.", connection.getDestinationIPAddress(), connection.getDestinationPort(), curSeq));
                        hostService.sendACK(Constants.REQUESTER.LOAD_BALANCER, header.getSeqNum());
                    }
                }
            } else {
                running = false;
            }
        }
    }

    private void processBrokerRequest(byte[] message, int action) {
        byte[] body = LBPacketHandler.getData(message);

        if (body != null) {
            Host broker = JSONDesrializer.fromJson(body, Host.class);

            if (broker != null && broker.isValid()) {
                if (action == Constants.TYPE.ADD.getValue()) {
                    logger.warn(String.format("[%s:%d] Received JOIN request from the broker. Adding the broker to the collection.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                    CacheManager.addBroker(broker);
                    sendAck();
                    logger.info(String.format("[%s:%d] Added the broker to the collection.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                } else if (action == Constants.TYPE.REM.getValue()) {
                    logger.warn(String.format("[%s:%d] Received REMOVE request from the broker. Removing the broker from the collection.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                    CacheManager.removeBroker(broker);
                    sendAck();
                } else {
                    logger.warn(String.format("[%s:%d] Received unsupported request type %s from the broker. Sending NACK packet.", connection.getDestinationIPAddress(), connection.getDestinationPort(), Constants.TYPE.values()[action].name()));
                    hostService.sendNACK(Constants.REQUESTER.LOAD_BALANCER, curSeq);
                }
            } else {
                logger.warn(String.format("[%s:%d] Received invalid broker information from the broker. Sending NACK packet.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                hostService.sendNACK(Constants.REQUESTER.LOAD_BALANCER, curSeq);
            }
        }
    }

    private void sendBrokerDetails(byte[] message) {
        byte[] body = LBPacketHandler.getData(message);
        byte[] response = null;

        if (body != null) {
            Request request = JSONDesrializer.fromJson(body, Request.class);

            if (request != null && request.isValid()) {
                if (request.getType() == Constants.REQUEST.TOPIC.getValue()) {
                    //Requesting for the entire topic
                    logger.info(String.format("[%s:%d] Received request to get all the broker information's handling the topic %s", connection.getDestinationIPAddress(), connection.getDestinationPort(), request.getTopicName()));
                    if (CacheManager.isTopicExist(request.getTopicName())) {
                        Topic topic = CacheManager.getTopic(request.getTopicName());
                        response = LBPacketHandler.createPacket(Constants.TYPE.RESP, topic);
                    } else {
                        //Topic don't exist
                        logger.warn(String.format("[%s:%d] There is no topic with the name %s exit. Sending NACK.", connection.getDestinationIPAddress(), connection.getDestinationPort(), request.getTopicName()));
                        hostService.sendNACK(Constants.REQUESTER.LOAD_BALANCER);
                    }
                } else {
                    //Requesting for a partition
                    logger.info(String.format("[%s:%d] Received request to get the broker information of a particular partition %d of a topic %s.", connection.getDestinationIPAddress(), connection.getDestinationPort(), request.getPartition(), request.getTopicName()));
                    if (CacheManager.isPartitionExist(request.getTopicName(), request.getPartition())) {
                        Partition partition = CacheManager.getPartition(request.getTopicName(), request.getPartition());
                        response = LBPacketHandler.createPacket(Constants.TYPE.RESP, partition);
                    } else {
                        //Partition don't exit
                        logger.warn(String.format("[%s:%d] There is no partition %d of the topic with the name as %s. Sending NACK", connection.getDestinationIPAddress(), connection.getDestinationPort(), request.getPartition(), request.getTopicName()));
                        hostService.sendNACK(Constants.REQUESTER.LOAD_BALANCER);
                    }
                }
            } else {
                logger.warn(String.format("[%s:%d] Received invalid request from the another end. Sending NACK", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                hostService.sendNACK(Constants.REQUESTER.LOAD_BALANCER);
            }
        }

        if (response != null) {
            connection.send(response);
        }
    }

    private void processTopicRequest(byte[] message, int action) {
        byte[] body = LBPacketHandler.getData(message);

        if (body != null) {
            Topic topic = JSONDesrializer.fromJson(message, Topic.class);

            if (topic != null && topic.isValid() && topic.getNumOfPartitions() > 0) {
                if (action == Constants.TYPE.ADD.getValue()) {
                    if (CacheManager.isTopicExist(topic.getName())) {
                        logger.warn(String.format("[%s:%d] Topic with the name %s already exist. Sending NACK.", connection.getDestinationIPAddress(), connection.getDestinationPort(), topic.getName()));
                        hostService.sendNACK(Constants.REQUESTER.LOAD_BALANCER, curSeq);
                    } else {
                        logger.debug(String.format("[%s:%d] Request received for distributing %d number of partitions of topic %s among %d number of brokers", connection.getDestinationIPAddress(), connection.getDestinationPort(), topic.getNumOfPartitions(), topic.getName(), CacheManager.getNumberOfBrokers()));
                        for (int index = 0; index < topic.getNumOfPartitions(); index++) {
                            Host broker = CacheManager.findBrokerWithLessLoad();
                            Partition partition = new Partition(topic.getName(), index, broker);
                            topic.addPartition(partition);

                            //logging
                            String log = String.format("[%s:%d] Broker %s:%d handling Topic %s - Partition %d.", connection.getDestinationIPAddress(), connection.getDestinationPort(), broker.getAddress(), broker.getPort(), topic.getName(), index);
                            logger.info(log);
                            System.out.println(log);
                        }

                        boolean flag = CacheManager.addTopic(topic);
                        if (flag) {
                            sendAck();
                            logger.debug(String.format("[%s:%d] Created topic %s with %d number of partitions. Sending the information to the respective brokers.",connection.getDestinationIPAddress(), connection.getDestinationPort(), topic.getName(), topic.getNumOfPartitions()));

                            //Send partition information to all the brokers asynchronously. Assuming that all the brokers might be up and running
                            sendToBrokers(topic);
                        } else {
                            //Two requests for creating topic with same name might have received. Another thread might have added that topic before.
                            logger.warn(String.format("[%s:%d] Another instance might have created same topic. Sending NACK", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                            hostService.sendNACK(Constants.REQUESTER.LOAD_BALANCER, curSeq);
                        }
                    }
                } else {
                    logger.warn(String.format("[%s:%d] Received unsupported action %d for the topic related service. Sending NACK", connection.getDestinationIPAddress(), connection.getDestinationPort(), action));
                    hostService.sendNACK(Constants.REQUESTER.LOAD_BALANCER, curSeq);
                }
            } else {
                logger.warn(String.format("[%s:%d] Received invalid requests to create the topic. Sending NACK", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                hostService.sendNACK(Constants.REQUESTER.LOAD_BALANCER, curSeq);
            }
        }
    }

    private void sendToBrokers(Topic topic) {
        HashMap<String, Topic> partitionsPerBroker = topic.groupBy();

        //Create threads equal to the number of r.
        ExecutorService threadPool = Executors.newFixedThreadPool(partitionsPerBroker.size());

        for (Map.Entry<String, Topic> entry : partitionsPerBroker.entrySet()) {
            //Per partition, assign a thread to send the partition information to the broker
            threadPool.execute(() -> sendToBroker(entry.getValue()));
        }

        threadPool.shutdown();
    }

    private void sendToBroker(Topic topic) {
        try {
            Host brokerInfo = topic.getPartitions().get(0).getBroker();
            Socket socket = new Socket(brokerInfo.getAddress(), brokerInfo.getPort());
            Connection connection = new Connection(socket, brokerInfo.getAddress(), brokerInfo.getPort(), this.connection.getSourceIPAddress(), this.connection.getSourcePort());
            if (connection.openConnection()) {
                logger.debug(String.format("[%s:%d] Sending the the topic %s - Partitions {%s} information to the broker.", connection.getDestinationIPAddress(), connection.getDestinationPort(), topic.getName(), topic.getPartitionString()));
                byte[] packet = LBPacketHandler.createPacket(Constants.TYPE.ADD, topic);
                hostService.sendPacketWithACK(packet, String.format("%s:%s", Constants.REQUESTER.LOAD_BALANCER.name(), Constants.TYPE.ADD.name()));
                //TODO: If isSuccess == false then, remove the topic information from the cache
                logger.info(String.format("[%s:%d] Send the topic %s - Partitions {%s} information to the broker.", connection.getDestinationIPAddress(), connection.getDestinationPort(), topic.getName(), topic.getPartitionString()));
            }
        } catch (IOException exception) {
            logger.error(String.format("[%s:%d] Fail to make connection with the broker in order to send the topic %s - Partitions {%s} information", connection.getDestinationIPAddress(), connection.getDestinationPort(), topic.getName(), topic.getPartitionString()), exception);
        }
    }

    private void sendAck() {
        hostService.sendACK(Constants.REQUESTER.LOAD_BALANCER, curSeq);
        curSeq = curSeq == 0 ? 1 : 0;
    }
}
