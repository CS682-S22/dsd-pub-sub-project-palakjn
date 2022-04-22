package controllers.loadBalancer;

import com.google.gson.reflect.TypeToken;
import configuration.Constants;
import configurations.BrokerConstants;
import controllers.Brokers;
import controllers.Channels;
import controllers.Connection;
import controllers.HostService;
import controllers.database.CacheManager;
import controllers.election.Election;
import controllers.heartbeat.HeartBeatSender;
import controllers.Broker;
import controllers.replication.SyncManager;
import models.*;
import models.data.File;
import models.heartbeat.HeartBeatRequest;
import models.requests.Request;
import models.responses.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.BrokerPacketHandler;
import utilities.JSONDesrializer;

import java.util.ArrayList;
import java.util.List;

/**
 * Responsible for handling requests from load balancer.
 *
 * @author Palak Jain
 */
public class LBHandler {
    private static final Logger logger = LogManager.getLogger(LBHandler.class);
    private HostService hostService;
    private Connection connection;
    private HeartBeatSender heartBeatSender;
    private Election election;
    private SyncManager syncManager;

    public LBHandler() {
        hostService = new HostService(logger);
        heartBeatSender = new HeartBeatSender();
        election = new Election();
        syncManager = new SyncManager();
    }

    /**
     * Send the request to load balancer to join the network
     */
    public boolean join() {
        return send(BrokerConstants.REQUEST_TYPE.ADD);
    }

    /**
     * Send the request to load balancer to remove it from the network
     */
    public boolean remove() {
        return send(BrokerConstants.REQUEST_TYPE.REM);
    }

    /**
     * Listen for requests from load balancer
     */
    public void listen() {
        if (connect()) {
            while (connection.isOpen()) {
                byte[] request = connection.receive();

                if (request != null) {
                    Header.Content header = BrokerPacketHandler.getHeader(request);

                    logger.info("Received request from Load Balancer.");
                    if (header != null) {
                        processRequest(header, request);
                    }
                }
            }
        }
    }

    /**
     * Process the request based on the action received from the load balancer
     */
    private void processRequest(Header.Content header, byte[] packet) {
        if (header.getType() == BrokerConstants.TYPE.REQ.getValue()) {
            //Adding new topic information
            byte[] body = BrokerPacketHandler.getData(packet);

            if (body != null) {
                Request<Topic> topicRequest = JSONDesrializer.deserializeRequest(body, new TypeToken<Request<Topic>>(){}.getType());
                Topic topic = null;

                if (topicRequest != null) {
                    topic = topicRequest.getRequest();
                }

                if (topic != null && topic.isValid()) {
                    if (topicRequest.getType().equalsIgnoreCase(BrokerConstants.REQUEST_TYPE.ADD)) {
                        logger.info(String.format("Received request to handle %d number of partitions for a topic %s.", topic.getNumOfPartitions(), topic.getName()));

                        //First checking if topic already exits
                        if (CacheManager.iSTopicExist(topic.getName())) {
                            if (isTopicWithPartitionsExist(topic)) {
                                logger.info(String.format("[%s:%d] Got the new broker update request for topic-partition %s.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort(), topic.getPartitionString()));
                                updateTopic(topic);
                            } else {
                                logger.warn(String.format("[%s] Broker handle the topic %s but not with the given partitions %s.", CacheManager.getBrokerInfo().getString(), topic.getName(), topic.getPartitionString()));
                            }
                        } else if (topic.getNumOfPartitions() > 0) {
                            logger.info(String.format("[%s] Got new topic information from load balancer. Updating the database", CacheManager.getBrokerInfo().getString()));
                            addTopic(topic);
                        } else {
                            logger.warn(String.format("[%s:%d] Received no partitions for the topic %s from the load balancer.", connection.getDestinationIPAddress(), connection.getDestinationPort(), topic.getName()));
                        }
                    }
                } else {
                    logger.warn(String.format("[%s:%d] Received invalid topic information from the load balancer.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                }
            } else {
                logger.warn(String.format("[%s:%d] Received empty request body from the producer.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
            }
        } else {
            logger.warn(String.format("[%s:%d] Received unsupported action %d from the load balancer.", connection.getDestinationIPAddress(), connection.getDestinationPort(), header.getType()));
        }
    }

    /**
     * Send new leader information to load balancer
     */
    public void sendLeaderUpdate(byte[] packet) {
        //Get the connection with load balancer
        if (connect()) {
            //Send leader info
            connection.send(packet);
        }
    }

    /**
     * Connect with the load balancer.
     */
    private boolean connect() {
        boolean isConnected = false;

        connection = Channels.get(null, BrokerConstants.CHANNEL_TYPE.LOADBALANCER);

        if (connection == null || !connection.isOpen()) {
            connection = hostService.connect(CacheManager.getLoadBalancer().getAddress(), CacheManager.getLoadBalancer().getPort());
        }

        if (connection != null && connection.isOpen()) {
            Channels.upsert(null, connection, BrokerConstants.CHANNEL_TYPE.LOADBALANCER);
            isConnected = true;
        }

        return isConnected;
    }

    /**
     * Add the new topic information to local DB
     */
    private void addTopic(Topic topic) {
        CacheManager.addTopic(topic.getName());

        for (Partition partition : topic.getPartitions()) {
            File file = new File(partition.getTopicName(), partition.getNumber());
            if (file.initialize(String.format(BrokerConstants.TOPIC_LOCATION, CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort()), partition.getTopicName(), partition.getNumber())) {
                CacheManager.addPartition(partition.getTopicName(), partition.getNumber(), file);

                if (partition.getLeader() != null) {
                    //Adding leader information of the given topic
                    CacheManager.setLeader(partition.getString(), new Broker(partition.getLeader()));
                }

                for (Host broker : partition.getBrokers()) {
                    CacheManager.addBroker(partition.getString(), new Broker(broker));

                    if (broker.isActive()) {
                        //Start sending heartbeat messages to another brokers
                        HeartBeatRequest heartBeatRequest = new HeartBeatRequest(partition.getString(), CacheManager.getBrokerInfo().getString(), broker.getHeartBeatString());
                        heartBeatSender.send(heartBeatRequest);
                    }
                }

                logger.info(String.format("[%s] The membership table for thr partition of the topic %s is %s.", CacheManager.getBrokerInfo().getString(), partition.getString(), partition.getMemberShipTable()));

                if (CacheManager.isLeader(partition.getString(), CacheManager.getBrokerInfo())) {
                    //If the current broker is leader of the topic then, change the status to READY
                    logger.info(String.format("[%s] Broker is the leader of the partition %s.", CacheManager.getBrokerInfo().getString(), partition.getString()));
                    syncManager.sync(partition.getString());
                } else {
                    //If the current broker is follower of the topic then,
                    if (!partition.getLeader().isActive()) {
                        // If the leader for the given partition is not active then, start the election
                        logger.info(String.format("[%s] New follower. Leader is inactive. Starting election to elect new leader.", CacheManager.getBrokerInfo().getString()));
                        election.start(partition.getString(), partition.getLeader());
                    } else {
                        // If the leader is active then, changing the status to SYNC mode and starting catching up with leader
                        logger.info(String.format("[%s] New follower. Sync data with the leader", CacheManager.getBrokerInfo().getString()));
                        syncManager.sync(partition.getString());
                    }
                }

                logger.info(String.format("[%s:%d] Added topic %s - partition %d information to the local cache.", connection.getDestinationIPAddress(), connection.getDestinationPort(), partition.getTopicName(), partition.getNumber()));
                System.out.printf("Handling topic %s - partition %d. Membership table: %s\n", partition.getTopicName(), partition.getNumber(), partition.getMemberShipTable());
            } else {
                logger.warn(String.format("[%s:%d] Fail to create directory for the topic- %s - Partition %d", connection.getDestinationIPAddress(), connection.getDestinationPort(), partition.getTopicName(), partition.getNumber()));
            }
        }
    }

    /**
     * Checking if new follower information received from load balancer and starting handshake with new follower
     */
    private void updateTopic(Topic topic) {
        for (Partition partition : topic.getPartitions()) {
            List<Host> brokersToSendHeartBeat = new ArrayList<>();

            Brokers receivedBrokers = new Brokers(partition.getBrokers());
            CacheManager.updateMembershipTable(partition.getString(), receivedBrokers, brokersToSendHeartBeat);

            for (Host broker : brokersToSendHeartBeat) {
                //Start sending heartbeat messages to the broker
                HeartBeatRequest heartBeatRequest = new HeartBeatRequest(partition.getString(), CacheManager.getBrokerInfo().getString(), broker.getHeartBeatString());
                heartBeatSender.send(heartBeatRequest);
            }

            if (!partition.getLeader().isActive()) {
                // If the leader for the given partition is not active then, start the election
                logger.info(String.format("[%s] Leader is failed for the partition %s. Starting election.", CacheManager.getBrokerInfo().getString(), partition.getString()));
                election.start(partition.getString(), partition.getLeader());
            } else {
                //If new follower is added, then keep the status of current broker for the partition as READY
                logger.debug(String.format("[%s] Set the status of the follower as READY after adding new broker information.", CacheManager.getBrokerInfo().getString()));
                CacheManager.setStatus(partition.getString(), BrokerConstants.BROKER_STATE.READY);
            }

            System.out.printf("Updated membership table for the topic %s - partition %d: %s\n", partition.getTopicName(), partition.getNumber(), partition.getMemberShipTable());
        }
    }

    /**
     * Checks whether the topic along with partitions exist
     */
    private boolean isTopicWithPartitionsExist(Topic topic) {
        boolean exist = true;

        for (Partition partition : topic.getPartitions()) {
            exist = CacheManager.isExist(partition.getTopicName(), partition.getNumber()) && exist;
        }

        return exist;
    }

    /**
     * Send the packet to the host and waits for an acknowledgement. Re-send the packet if time-out
     */
    private boolean send(String type) {
        boolean isSuccess = false;

        if (connect()) {
            boolean running = true;
            byte[] packet = BrokerPacketHandler.createPacket(CacheManager.getBrokerInfo(), type);

            connection.send(packet);
            connection.setTimer(Constants.ACK_WAIT_TIME);

            while (running && connection.isOpen()) {
                byte[] responseBytes = connection.receive();

                if (responseBytes != null) {
                    Header.Content header = BrokerPacketHandler.getHeader(responseBytes);

                    if (header != null) {
                        if (header.getType() == Constants.TYPE.ACK.getValue() && type.equalsIgnoreCase(BrokerConstants.REQUEST_TYPE.REM)) {
                            logger.info(String.format("[%s:%d] Received an acknowledgment for the %s request from the host %s:%d.", connection.getSourceIPAddress(), connection.getSourcePort(), type, connection.getDestinationIPAddress(), connection.getDestinationPort()));
                            running = false;
                            isSuccess = true;
                        } else if (header.getType() == Constants.TYPE.NACK.getValue() && type.equalsIgnoreCase(BrokerConstants.REQUEST_TYPE.REM)) {
                            logger.warn(String.format("[%s:%d] Received negative acknowledgment for the %s request from the host %s:%d. Not retrying.", connection.getSourceIPAddress(), connection.getSourcePort(), type, connection.getDestinationIPAddress(), connection.getDestinationPort()));
                            running = false;
                        } else if (header.getType() == Constants.TYPE.RESP.getValue() && type.equalsIgnoreCase(Constants.REQUEST_TYPE.ADD)) {
                            logger.info(String.format("[%s:%d] Received a response for the %s request from the host %s:%d.", connection.getSourceIPAddress(), connection.getSourcePort(), type, connection.getDestinationIPAddress(), connection.getDestinationPort()));

                            running = !getPriorityNum(responseBytes);
                            isSuccess = !running;
                        } else {
                            logger.warn(String.format("[%s:%d] Received wrong packet type i.e. %d for the %s request from the host %s:%d. Retrying.", connection.getSourceIPAddress(), connection.getSourcePort(), header.getType(), type, connection.getDestinationIPAddress(), connection.getDestinationPort()));
                            connection.send(packet);
                        }
                    } else {
                        logger.warn(String.format("[%s:%d] Received invalid header for the %s request from the host %s:%d. Retrying.", connection.getSourceIPAddress(), connection.getSourcePort(), type, connection.getDestinationIPAddress(), connection.getDestinationPort()));
                        connection.send(packet);
                    }
                } else if (connection.isOpen()) {
                    logger.warn(String.format("[%s:%d] Time-out happen for the packet %s to the host %s:%d. Re-sending the packet.", connection.getSourceIPAddress(), connection.getSourcePort(), type, connection.getDestinationIPAddress(), connection.getDestinationPort()));
                    connection.send(packet);
                } else {
                    logger.warn(String.format("[%s:%d] Connection is closed by the receiving end %s:%d. Failed to send %s packet", connection.getSourceIPAddress(), connection.getSourcePort(), connection.getDestinationIPAddress(), connection.getDestinationPort(), type));
                }
            }

            connection.resetTimer();
        }

        return isSuccess;
    }

    /**
     * Get the priorityNumber from the response
     */
    private boolean getPriorityNum(byte[] responseBytes) {
        boolean isSet = false;

        byte[] body = BrokerPacketHandler.getData(responseBytes);

        if (body != null) {
            Response<JoinResponse> joinResponse = JSONDesrializer.deserializeResponse(body, new TypeToken<Response<JoinResponse>>(){}.getType());

            if (joinResponse != null && joinResponse.isValid() && joinResponse.isOk()) {
                CacheManager.setPriorityNum(joinResponse.getObject().getPriorityNum());
                logger.info(String.format("[%s] Joined the network with the priority number is %d.", CacheManager.getBrokerInfo().getString(), CacheManager.getPriorityNum()));
                System.out.printf("[%s] Joined the network with the priority number is %d. \n", CacheManager.getBrokerInfo().getString(), CacheManager.getPriorityNum());
                isSet = true;
            }
        }

        return isSet;
    }
}
