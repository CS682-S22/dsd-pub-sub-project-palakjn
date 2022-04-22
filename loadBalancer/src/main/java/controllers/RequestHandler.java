package controllers;

import com.google.gson.reflect.TypeToken;
import configuration.Constants;
import models.*;
import models.requests.CreateTopicRequest;
import models.requests.BrokerUpdateRequest;
import models.requests.GetBrokerRequest;
import models.requests.Request;
import models.responses.Response;
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
                    logger.info(String.format("[%s:%d] Received request from broker", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                    processBrokerRequest(request, Constants.TYPE.values()[header.getType()].name());
                } else if ((header.getRequester() == Constants.REQUESTER.PRODUCER.getValue() ||
                        header.getRequester() == Constants.REQUESTER.CONSUMER.getValue()) &&
                        header.getType() == Constants.TYPE.REQ.getValue()) {
                    logger.info(String.format("[%s:%d] Received request from %s to get broker details for a partition/topic.", connection.getDestinationIPAddress(), connection.getDestinationPort(), Constants.REQUESTER.values()[header.getRequester()].name()));
                    sendBrokerDetails(request);
                } else if (header.getRequester() == Constants.REQUESTER.TOPIC.getValue()) {
                    logger.info(String.format("[%s:%d] Received request from host to create the topic", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                    processTopicRequest(request);
                }
            } else {
                running = false;
            }
        }
    }

    /**
     * Process the request from broker to add/remove it from the network
     */
    private void processBrokerRequest(byte[] message, String type) {
        byte[] body = LBPacketHandler.getData(message);

        if (body != null) {
            if (type.equals(Constants.TYPE.REQ.name())) {
                Request<Host> request = JSONDesrializer.deserializeRequest(body, new TypeToken<Request<Host>>(){}.getType());

                if (request != null) {
                    Host broker = request.getRequest();

                    if (broker != null && broker.isValid()) {
                        if (request.getType().equalsIgnoreCase(Constants.REQUEST_TYPE.ADD)) {
                            logger.warn(String.format("[%s:%d] Received JOIN request from the broker. Adding the broker to the collection.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                            broker = CacheManager.addBroker(broker);

                            //Saving the connection between broker and load-balancer for future updates
                            Channels.add(broker.getString(), connection, Constants.CHANNEL_TYPE.BROKER);

                            //Sending response to the broker with the priority number
                            sendJoinResponse(broker);

                            logger.info(String.format("[%s:%d] Added the broker with %d priority number to the collection.", connection.getDestinationIPAddress(), connection.getDestinationPort(), broker.getPriorityNum()));
                            System.out.printf("Broker %s:%d joined the network.\n", connection.getDestinationIPAddress(), connection.getDestinationPort());
                        } else if (request.getType().equalsIgnoreCase(Constants.REQUEST_TYPE.REM)) {
                            logger.warn(String.format("[%s:%d] Received REMOVE request from the broker. Removing the broker from the collection.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                            CacheManager.removeBroker(broker);
                            hostService.sendACK(connection, Constants.REQUESTER.LOAD_BALANCER);
                        } else {
                            logger.warn(String.format("[%s:%d] Received unsupported request type %s from the broker. Sending NACK packet.", connection.getDestinationIPAddress(), connection.getDestinationPort(), request.getType()));
                            hostService.sendNACK(connection, Constants.REQUESTER.LOAD_BALANCER);
                        }
                    } else {
                        logger.warn(String.format("[%s:%d] Received invalid broker information from the broker. Sending NACK packet.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                        hostService.sendNACK(connection, Constants.REQUESTER.LOAD_BALANCER);
                    }
                }
            } else if (type.equals(Constants.TYPE.UPDATE.name())) {
                Request<BrokerUpdateRequest> request = JSONDesrializer.deserializeRequest(body, new TypeToken<Request<BrokerUpdateRequest>>(){}.getType());

                if (request != null) {
                    BrokerUpdateRequest brokerUpdateRequest = request.getRequest();

                    if (brokerUpdateRequest != null && brokerUpdateRequest.isValid()) {
                        if (request.getType().equalsIgnoreCase(Constants.REQUEST_TYPE.FAIL)) {
                            logger.info(String.format("[%s:%d] Received request to indicate the failure of broker %s:%d handling topic %s:%d", connection.getDestinationIPAddress(), connection.getDestinationPort(), brokerUpdateRequest.getBroker().getAddress(), brokerUpdateRequest.getBroker().getPort(), brokerUpdateRequest.getTopic(), brokerUpdateRequest.getPartition()));

                            logger.info(String.format("[%s:%d] Marking broker %s inactive and finding new follower.", connection.getDestinationIPAddress(), connection.getDestinationPort(), brokerUpdateRequest.getBroker().getString()));
                            Topic topic = CacheManager.updateMDAfterFailure(brokerUpdateRequest);

                            if (topic != null) {
                                sendToBrokers(topic);
                            }
                        } else if (request.getType().equalsIgnoreCase(Constants.REQUEST_TYPE.LEADER)) {
                            logger.info(String.format("[%s:%d] Received request to set the broker %s:%d as a leader which will handle the topic %s:%d", connection.getDestinationIPAddress(), connection.getDestinationPort(), brokerUpdateRequest.getBroker().getAddress(), brokerUpdateRequest.getBroker().getPort(), brokerUpdateRequest.getTopic(), brokerUpdateRequest.getPartition()));
                            CacheManager.updateLeader(brokerUpdateRequest);
                        }
                    }
                }
            } else {
                logger.warn(String.format("[%s:%d] Received unsupported header type %s from the broker. Sending NACK packet.", connection.getDestinationIPAddress(), connection.getDestinationPort(), type));
                hostService.sendNACK(connection, Constants.REQUESTER.LOAD_BALANCER);
            }
        }
    }

    /**
     * Send broker details to producer/consumer which is holding the given partition of the topic.
     */
    private void sendBrokerDetails(byte[] message) {
        byte[] body = LBPacketHandler.getData(message);
        byte[] responseBytes = null;

        if (body != null) {
            Request<GetBrokerRequest> request = JSONDesrializer.deserializeRequest(body, new TypeToken<Request<GetBrokerRequest>>(){}.getType());
            GetBrokerRequest getBrokerRequest = null;

            if (request != null) {
                getBrokerRequest = request.getRequest();
            }

            if (getBrokerRequest != null && getBrokerRequest.isValid()) {
                //Requesting for a partition
                logger.info(String.format("[%s:%d] Received request to get the broker information of a particular partition %d of a topic %s.", connection.getDestinationIPAddress(), connection.getDestinationPort(), getBrokerRequest.getPartition(), getBrokerRequest.getName()));
                if (CacheManager.isPartitionExist(getBrokerRequest.getName(), getBrokerRequest.getPartition())) {
                    Host leader = CacheManager.getLeader(getBrokerRequest.getName(), getBrokerRequest.getPartition());
                    Response<Host> response;

                    if (leader.isActive()) {
                        response = new Response<>(Constants.RESPONSE_STATUS.OK, leader);
                    } else {
                        response = new Response<>(Constants.RESPONSE_STATUS.SYN);
                    }

                    responseBytes = LBPacketHandler.createPacket(Constants.TYPE.RESP, response);
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

        if (responseBytes != null) {
            connection.send(responseBytes);
        }
    }

    /**
     * Process the request to create new topic
     */
    private void processTopicRequest(byte[] message) {
        byte[] body = LBPacketHandler.getData(message);

        if (body != null) {
            Request<CreateTopicRequest> request = JSONDesrializer.deserializeRequest(body, new TypeToken<Request<CreateTopicRequest>>(){}.getType());
            CreateTopicRequest topicRequest = null;

            if (request != null && request.getRequest() != null) {
                topicRequest = request.getRequest();
            }

            if (topicRequest != null && topicRequest.isValid()) {
                if (request.getType().equalsIgnoreCase(Constants.REQUEST_TYPE.ADD)) {
                    createTopic(topicRequest);
                } else {
                    logger.warn(String.format("[%s:%d] Received unsupported action %s for the topic related service. Sending NACK", connection.getDestinationIPAddress(), connection.getDestinationPort(), request.getType()));
                    hostService.sendNACK(connection, Constants.REQUESTER.LOAD_BALANCER);
                }
            } else {
                logger.warn(String.format("[%s:%d] Received invalid requests to create the topic. Sending NACK", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                hostService.sendNACK(connection, Constants.REQUESTER.LOAD_BALANCER);
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
            hostService.sendNACK(connection, Constants.REQUESTER.LOAD_BALANCER);
        } else if (CacheManager.getNumberOfBrokers() < (topic.getNumOfFollowers() + 1)) {
            logger.warn(String.format("[%s:%d] Less number of brokers %d available for creating topic with the name %s. Sending NACK.", connection.getDestinationIPAddress(), connection.getDestinationPort(), CacheManager.getNumberOfBrokers(), topic.getName()));
            hostService.sendNACK(connection, Constants.REQUESTER.LOAD_BALANCER);
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
            hostService.sendACK(connection, Constants.REQUESTER.LOAD_BALANCER);
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

        if (!partitionsPerBroker.isEmpty()) {
            //Create threads equal to the number of brokers.
            ExecutorService threadPool = Executors.newFixedThreadPool(partitionsPerBroker.size());

            for (Map.Entry<String, Topic> entry : partitionsPerBroker.entrySet()) {
                //Per partition, assign a thread to send the partition information to the broker
                threadPool.execute(() -> sendToBroker(entry.getKey(), entry.getValue()));
            }

            threadPool.shutdown();
        }
    }

    /**
     * Send the partitions' information of a topic to the broker which is going to handle them.
     */
    private void sendToBroker(String brokerKey, Topic topic) {
        Host brokerInfo = CacheManager.getBroker(brokerKey);
        Connection connection = connect(brokerInfo);

        if (connection != null && connection.isOpen()) {
            logger.debug(String.format("[%s:%d] Sending the the topic %s - Partitions {%s} information to the broker %s:.", connection.getSourceIPAddress(), connection.getSourcePort(), topic.getName(), topic.getPartitionString(), brokerKey));
            Request<Topic> request = new Request<>(Constants.REQUEST_TYPE.ADD, topic);
            byte[] packet = LBPacketHandler.createPacket(Constants.TYPE.REQ, request);
            boolean isSuccess = connection.send(packet);
            if (isSuccess) {
                logger.info(String.format("[%s:%d] Send the topic %s - Partitions {%s} information to the broker.", connection.getDestinationIPAddress(), connection.getDestinationPort(), topic.getName(), topic.getPartitionString()));
            } else {
                //Removing topic information from the cache.
                CacheManager.removePartitions(topic);
            }
        }
    }

    /**
     * Connect with the broker
     */
    private Connection connect(Host brokerInfo) {
        Connection connection = Channels.get(brokerInfo.getString(), Constants.CHANNEL_TYPE.BROKER);

        if (connection == null || !connection.isOpen()) {
            connection = hostService.connect(brokerInfo.getAddress(), brokerInfo.getPort());
        }

        if (connection != null && connection.isOpen()) {
            Channels.upsert(brokerInfo.getString(), connection, Constants.CHANNEL_TYPE.BROKER);
        }

        return connection;
    }

    /**
     * Send JOIN response with the priority number to the broker
     */
    private void sendJoinResponse(Host broker) {
        byte[] response = LBPacketHandler.createJoinResponse(broker.getPriorityNum());
        connection.send(response);
    }
}
