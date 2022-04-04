package controllers;

import configuration.Constants;
import models.*;
import models.requests.CreateTopicRequest;
import models.requests.GetBrokerRequest;
import models.requests.Request;
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

/**
 * Responsible for handling requests from another host.
 *
 * @author Palak Jain
 */
public class RequestHandler {
    private static final Logger logger = LogManager.getLogger(RequestHandler.class);
    private HostService hostService;
    private Connection connection;
    private int curSeq = 0;

    public RequestHandler(Connection connection) {
        this.connection = connection;
        this.hostService = new HostService(logger);
    }

    /**
     * Gets the request from another host and process them based on requester and action
     */
    public void process() {
        boolean running = true;

        while (running) {
            byte[] request = connection.receive();

            if (request != null) {
                Header.Content header = LBPacketHandler.getHeader(request);

                if (header.getRequester() == Constants.REQUESTER.BROKER.getValue()) {
                    if (header.getSeqNum() == curSeq) {
                        logger.info(String.format("[%s:%d] Received request from broker with sequence number: %d", connection.getDestinationIPAddress(), connection.getDestinationPort(), curSeq));
                        processBrokerRequest(request);
                    } else if (header.getSeqNum() < curSeq) {
                        logger.info(String.format("[%s:%d] Received another same request from broker with sequence number: %d. Sending acknowledgement.", connection.getDestinationIPAddress(), connection.getDestinationPort(), curSeq));
                        hostService.sendACK(connection, Constants.REQUESTER.LOAD_BALANCER, header.getSeqNum());
                    }
                } else if ((header.getRequester() == Constants.REQUESTER.PRODUCER.getValue() ||
                        header.getRequester() == Constants.REQUESTER.CONSUMER.getValue()) &&
                        header.getType() == Constants.TYPE.REQ.getValue()) {
                    logger.info(String.format("[%s:%d] Received request from %s to get broker details for a partition/topic.", connection.getDestinationIPAddress(), connection.getDestinationPort(), Constants.REQUESTER.values()[header.getRequester()].name()));
                    sendBrokerDetails(request);
                } else if (header.getRequester() == Constants.REQUESTER.TOPIC.getValue()) {
                    if (header.getSeqNum() == curSeq) {
                        logger.info(String.format("[%s:%d] Received request from host with sequence number: %d to create the topic", connection.getDestinationIPAddress(), connection.getDestinationPort(), curSeq));
                        processTopicRequest(request);
                    } else if (header.getSeqNum() < curSeq) {
                        logger.info(String.format("[%s:%d] Received another same request from host with sequence number: %d to create the topic. Sending an acknowledgement.", connection.getDestinationIPAddress(), connection.getDestinationPort(), curSeq));
                        hostService.sendACK(connection, Constants.REQUESTER.LOAD_BALANCER, header.getSeqNum());
                    }
                }
            } else {
                running = false;
            }
        }
    }

    /**
     * Process the request from broker to add/remove it from the network
     */
    private void processBrokerRequest(byte[] message) {
        byte[] body = LBPacketHandler.getData(message);

        if (body != null) {
            Request<Host> request = JSONDesrializer.fromJson(body, Request.class);
            Host broker = null;

            if (request != null) {
                broker = request.getRequest();
            }

            if (broker != null && broker.isValid()) {
                if (request.getType().equalsIgnoreCase(Constants.REQUEST_TYPE.ADD)) {
                    logger.warn(String.format("[%s:%d] Received JOIN request from the broker. Adding the broker to the collection.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                    broker = CacheManager.addBroker(broker);

                    //Sending response to the broker with the priority number
                    sendJoinResponse(broker);

                    logger.info(String.format("[%s:%d] Added the broker with %d priority number to the collection.", connection.getDestinationIPAddress(), connection.getDestinationPort(), broker.getPriorityNum()));
                    System.out.printf("Broker %s:%d joined the network.\n", connection.getDestinationIPAddress(), connection.getDestinationPort());
                } else if (request.getType().equalsIgnoreCase(Constants.REQUEST_TYPE.REM)) {
                    logger.warn(String.format("[%s:%d] Received REMOVE request from the broker. Removing the broker from the collection.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                    CacheManager.removeBroker(broker);
                    sendAck();
                } else {
                    logger.warn(String.format("[%s:%d] Received unsupported request type %s from the broker. Sending NACK packet.", connection.getDestinationIPAddress(), connection.getDestinationPort(), request.getType()));
                    hostService.sendNACK(connection, Constants.REQUESTER.LOAD_BALANCER, curSeq);
                }
            } else {
                logger.warn(String.format("[%s:%d] Received invalid broker information from the broker. Sending NACK packet.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                hostService.sendNACK(connection, Constants.REQUESTER.LOAD_BALANCER, curSeq);
            }
        }
    }

    /**
     * Send broker details to producer/consumer which is holding the given partition of the topic.
     */
    private void sendBrokerDetails(byte[] message) {
        byte[] body = LBPacketHandler.getData(message);
        byte[] response = null;

        if (body != null) {
            Request<GetBrokerRequest> request = JSONDesrializer.fromJson(body, Request.class);
            GetBrokerRequest getBrokerRequest = null;

            if (request != null) {
                getBrokerRequest = request.getRequest();
            }

            if (getBrokerRequest != null && getBrokerRequest.isValid()) {
                //Requesting for a partition
                logger.info(String.format("[%s:%d] Received request to get the broker information of a particular partition %d of a topic %s.", connection.getDestinationIPAddress(), connection.getDestinationPort(), getBrokerRequest.getPartition(), getBrokerRequest.getName()));
                if (CacheManager.isPartitionExist(getBrokerRequest.getName(), getBrokerRequest.getPartition())) {
                    Partition partition = CacheManager.getPartition(getBrokerRequest.getName(), getBrokerRequest.getPartition());
                    response = LBPacketHandler.createPacket(Constants.TYPE.RESP, partition);
                } else {
                    //Partition don't exit
                    logger.warn(String.format("[%s:%d] There is no partition %d of the topic with the name as %s. Sending NACK", connection.getDestinationIPAddress(), connection.getDestinationPort(), getBrokerRequest.getPartition(), getBrokerRequest.getName()));
                    hostService.sendNACK(connection, Constants.REQUESTER.LOAD_BALANCER);
                }
            } else {
                logger.warn(String.format("[%s:%d] Received invalid request from the another end. Sending NACK", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                hostService.sendNACK(connection, Constants.REQUESTER.LOAD_BALANCER);
            }
        }

        if (response != null) {
            connection.send(response);
        }
    }

    /**
     * Process the request to create new topic
     */
    private void processTopicRequest(byte[] message) {
        byte[] body = LBPacketHandler.getData(message);

        if (body != null) {
            Request<CreateTopicRequest> request = JSONDesrializer.fromJson(body, Request.class);
            CreateTopicRequest topicRequest = null;

            if (request != null && request.getRequest() != null) {
                topicRequest = request.getRequest();
            }

            if (topicRequest != null && topicRequest.isValid()) {
                if (request.getType().equalsIgnoreCase(Constants.REQUEST_TYPE.ADD)) {
                    createTopic(topicRequest);
                } else {
                    logger.warn(String.format("[%s:%d] Received unsupported action %d for the topic related service. Sending NACK", connection.getDestinationIPAddress(), connection.getDestinationPort(), request.getType()));
                    hostService.sendNACK(connection, Constants.REQUESTER.LOAD_BALANCER, curSeq);
                }
            } else {
                logger.warn(String.format("[%s:%d] Received invalid requests to create the topic. Sending NACK", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                hostService.sendNACK(connection, Constants.REQUESTER.LOAD_BALANCER, curSeq);
            }
        }
    }

    /**
     * Create new topic with the partitions.
     * Allocate partitions to the available broker based on the load on each.
     * Send topic-partition information to brokers which are going to handle them.
     */
    private synchronized void createTopic(CreateTopicRequest topic) {
        if (CacheManager.isTopicExist(topic.getName())) {
            logger.warn(String.format("[%s:%d] Topic with the name %s already exist. Sending NACK.", connection.getDestinationIPAddress(), connection.getDestinationPort(), topic.getName()));
            hostService.sendNACK(connection, Constants.REQUESTER.LOAD_BALANCER, curSeq);
        } else if (CacheManager.getNumberOfBrokers() < (topic.getNumOfFollowers() + 1)) {
            logger.warn(String.format("[%s:%d] Less number of brokers %d available for creating topic with the name %s. Sending NACK.", connection.getDestinationIPAddress(), connection.getDestinationPort(), CacheManager.getNumberOfBrokers(), topic.getName()));
            hostService.sendNACK(connection, Constants.REQUESTER.LOAD_BALANCER, curSeq);
        } else {
            logger.debug(String.format("[%s:%d] Request received for distributing %d number of partitions of topic %s among %d number of brokers", connection.getDestinationIPAddress(), connection.getDestinationPort(), topic.getNumOfPartitions(), topic.getName(), CacheManager.getNumberOfBrokers()));
            Topic newTopic = new Topic(topic.getName(), topic.getNumOfFollowers());

            for (int index = 0; index < topic.getNumOfPartitions(); index++) {
                //Getting brokers which will handle the partition of the topic
                Brokers brokers = CacheManager.findBrokersWithLessLoad(topic.getNumOfFollowers() + 1);

                //Creating new partition and adding to the topic
                Partition partition = new Partition(topic.getName(), index, brokers.getLeader(), brokers.getBrokers());
                newTopic.addPartition(partition);

                //logging
                String log = String.format("[%s:%d] Brokers: {%s} handling Topic %s - Partition %d.", connection.getDestinationIPAddress(), connection.getDestinationPort(), brokers, topic.getName(), index);
                logger.info(log);
                System.out.println(log);
            }

            CacheManager.addTopic(newTopic);
            sendAck();
            logger.debug(String.format("[%s:%d] Created topic %s with %d number of partitions. Sending the information to the respective brokers.",connection.getDestinationIPAddress(), connection.getDestinationPort(), newTopic.getName(), newTopic.getNumOfPartitions()));

            //Send partition information to all the brokers asynchronously. Assuming that all the brokers might be up and running
            sendToBrokers(newTopic);
        }
    }

    /**
     * Send partitions information of a topic to respective brokers
     */
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

    /**
     * Send the partitions' information of a topic to the broker which is going to handle them.
     */
    private void sendToBroker(Topic topic) {
        try {
            Host brokerInfo = topic.getPartitions().get(0).getLeader();
            Socket socket = new Socket(brokerInfo.getAddress(), brokerInfo.getPort());
            Connection connection = new Connection(socket, brokerInfo.getAddress(), brokerInfo.getPort(), this.connection.getSourceIPAddress(), this.connection.getSourcePort());
            if (connection.openConnection()) {
                logger.debug(String.format("[%s:%d] Sending the the topic %s - Partitions {%s} information to the broker.", connection.getDestinationIPAddress(), connection.getDestinationPort(), topic.getName(), topic.getPartitionString()));
                Request<Topic> request = new Request<>(Constants.REQUEST_TYPE.ADD, topic);
                byte[] packet = LBPacketHandler.createPacket(Constants.TYPE.REQ, request);
                boolean isSuccess = hostService.sendPacketWithACK(connection, packet, String.format("%s:%s", Constants.REQUESTER.LOAD_BALANCER.name(), Constants.TYPE.REQ.name()));
                if (isSuccess) {
                    logger.info(String.format("[%s:%d] Send the topic %s - Partitions {%s} information to the broker.", connection.getDestinationIPAddress(), connection.getDestinationPort(), topic.getName(), topic.getPartitionString()));
                } else {
                    //Removing topic information from the cache.
                    CacheManager.removePartitions(topic);
                }
            }
        } catch (IOException exception) {
            logger.error(String.format("[%s:%d] Fail to make connection with the broker in order to send the topic %s - Partitions {%s} information", connection.getDestinationIPAddress(), connection.getDestinationPort(), topic.getName(), topic.getPartitionString()), exception);
        }
    }

    /**
     * Send an acknowledgement and increment the sequence number which is expected to read for next request
     */
    private void sendAck() {
        hostService.sendACK(connection, Constants.REQUESTER.LOAD_BALANCER, curSeq);
        curSeq++;
    }

    private void sendJoinResponse(Host broker) {
        byte[] response = LBPacketHandler.createJoinResponse(broker.getPriorityNum());
        connection.send(response);
        curSeq++;
    }
}
