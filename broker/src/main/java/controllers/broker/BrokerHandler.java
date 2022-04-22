package controllers.broker;

import com.google.gson.reflect.TypeToken;
import configurations.BrokerConstants;
import controllers.*;
import controllers.database.CacheManager;
import controllers.election.Election;
import controllers.heartbeat.HeartBeatReceiver;
import controllers.heartbeat.HeartBeatSender;
import controllers.replication.SyncManager;
import models.Header;
import models.data.File;
import models.requests.BrokerUpdateRequest;
import models.requests.Request;
import models.requests.TopicReadWriteRequest;
import models.sync.DataPacket;
import models.sync.OffsetRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.BrokerPacketHandler;
import utilities.JSONDesrializer;

import java.util.List;
import java.util.Objects;

/**
 * Responsible for handling requests from other brokers
 *
 * @author Palak Jain
 */
public class BrokerHandler {
    private static final Logger logger = LogManager.getLogger(BrokerHandler.class);
    private Connection connection;
    private HostService hostService;
    private SyncManager syncManager;
    private DataTransfer dataTransfer;
    private HeartBeatSender heartBeatSender;

    public BrokerHandler(Connection connection) {
        this.connection = connection;
        hostService = new HostService(logger);
        syncManager = new SyncManager();
        dataTransfer = new DataTransfer(connection);
        heartBeatSender = new HeartBeatSender();
    }

    /**
     * Process the requests from another broker based on type
     */
    public void handleRequest(Header.Content header, byte[] message) {
        if (header.getType() == BrokerConstants.TYPE.HEARTBEAT.getValue()) {
            HeartBeatReceiver heartBeatReceiver = new HeartBeatReceiver();
            heartBeatReceiver.handleRequest(connection, message);
        } else if (header.getType() == BrokerConstants.TYPE.ELECTION.getValue()) {
            logger.info(String.format("[%s] Received \"Election\" message from the broker.", CacheManager.getBrokerInfo().getString()));
            hostService.sendACK(connection, BrokerConstants.REQUESTER.BROKER);

            Election election = new Election();
            election.handleRequest(message);
        } else if (header.getType() == BrokerConstants.TYPE.UPDATE.getValue()) {
            logger.info(String.format("[%s] Received \"Update\" request from broker", CacheManager.getBrokerInfo().getString()));
            processUpdateRequest(message);
        } else if (header.getType() == BrokerConstants.TYPE.REQ.getValue()) {
            logger.info(String.format("[%s] Received \"REQ\" request from broker", CacheManager.getBrokerInfo().getString()));
            processREQRequest(message);
        } else if (header.getType() == BrokerConstants.TYPE.PULL.getValue()) {
            logger.info(String.format("[%s] Received \"PULL\" request from broker.", CacheManager.getBrokerInfo().getString()));
            processPullRequest(message);
        } else if (header.getType() == BrokerConstants.TYPE.DATA.getValue()) {
            logger.info(String.format("[%s] Received \"DATA\" request from broker.", CacheManager.getBrokerInfo().getString()));
            processDataRequest(message);
        }
    }

    /**
     * Process requests from broker with header type as Update (Updating leader, etc.)
     */
    private void processUpdateRequest(byte[] message) {
        byte[] data = BrokerPacketHandler.getData(message);

        if (data != null) {
            Request<BrokerUpdateRequest> brokerUpdateRequest = JSONDesrializer.deserializeRequest(data, new TypeToken<Request<BrokerUpdateRequest>>(){}.getType());

            if (brokerUpdateRequest != null && brokerUpdateRequest.getRequest() != null) {
                if (Objects.equals(brokerUpdateRequest.getType(), BrokerConstants.REQUEST_TYPE.LEADER)) {
                    logger.info(String.format("[%s] New leader %s being elected for the partition %s", CacheManager.getBrokerInfo().getString(), brokerUpdateRequest.getRequest().getBroker().getString(), brokerUpdateRequest.getRequest().getKey()));
                    CacheManager.setLeader(brokerUpdateRequest.getRequest().getKey(), new Broker(brokerUpdateRequest.getRequest().getBroker()));
                    hostService.sendACK(connection, BrokerConstants.REQUESTER.BROKER);

                    logger.info(String.format("[%s] The membership table for the partition of the topic %s after new leader is %s.", CacheManager.getBrokerInfo().getString(), brokerUpdateRequest.getRequest().getKey(), CacheManager.getMemberShipTable(brokerUpdateRequest.getRequest().getKey())));
                    System.out.printf("[%s] The membership table for the partition of the topic %s after new leader is %s.%n", CacheManager.getBrokerInfo().getString(), brokerUpdateRequest.getRequest().getKey(), CacheManager.getMemberShipTable(brokerUpdateRequest.getRequest().getKey()));

                    syncManager.sync(brokerUpdateRequest.getRequest().getKey());
                } else {
                    logger.warn(String.format("[%s] Receive unsupported broker update request packet %s.", CacheManager.getBrokerInfo().getString(), brokerUpdateRequest.getType()));
                }
            }
        }
    }

    /**
     * Process requests from broker with header type as REQ (Getting offset, etc.)
     */
    private void processREQRequest(byte[] message) {
        byte[] data = BrokerPacketHandler.getData(message);

        if (data != null) {
            Request<OffsetRequest> request = JSONDesrializer.deserializeRequest(data, new TypeToken<Request<OffsetRequest>>(){}.getType());

            if (request != null && request.getRequest() != null && request.getRequest().isValid()) {
                if (request.getType().equalsIgnoreCase(BrokerConstants.REQUEST_TYPE.OFFSET)) {
                    OffsetRequest offsetRequest = request.getRequest();
                    logger.info(String.format("[%s] Request received to get the offset for the partition %s.", CacheManager.getBrokerInfo().getString(), offsetRequest.getKey()));

                    File partition = CacheManager.getPartition(offsetRequest.getKey());

                    if (partition != null) {
                        byte[] response = BrokerPacketHandler.createOffsetResponse(offsetRequest.getKey(), partition.getOffset(), partition.getTotalSize());
                        connection.send(response);
                        logger.info(String.format("[%s] Send offset %d and total size as %d for the partition %s.", CacheManager.getBrokerInfo().getString(), partition.getOffset(), partition.getTotalSize(), offsetRequest.getKey()));
                    } else {
                        logger.info(String.format("[%s] Not able to send an offset response as broker not holding the partition %s.", CacheManager.getBrokerInfo().getString(), offsetRequest.getKey()));
                        hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER);
                    }
                } else {
                    logger.info(String.format("[%s] Received unsupported request type %s for REQ type", CacheManager.getBrokerInfo().getString(), request.getType()));
                }
            } else {
                logger.info(String.format("[%s] Received invalid REQ request", CacheManager.getBrokerInfo().getString()));

                if (request != null) {
                    logger.info("Received request type is: " + request.getType());
                }
            }
        }
    }

    /**
     * Process requests from broker with header type as PULL (Send data from given offset, etc.)
     */
    private void processPullRequest(byte[] message) {
        byte[] data = BrokerPacketHandler.getData(message);

        if (data != null) {
            Request<TopicReadWriteRequest> request = JSONDesrializer.deserializeRequest(data, new TypeToken<Request<TopicReadWriteRequest>>(){}.getType());

            if (request != null && request.getRequest() != null && request.getRequest().isValid()) {
                TopicReadWriteRequest topicReadWriteRequest = request.getRequest();
                logger.info(String.format("[%s] Received PULL request to send data to broker for partition %s from offset %s to offset %s", CacheManager.getBrokerInfo().getString(), topicReadWriteRequest.getName(), topicReadWriteRequest.getFromOffset(), topicReadWriteRequest.getToOffset()));
                if (CacheManager.isExist(topicReadWriteRequest.getName(), topicReadWriteRequest.getPartition())) {
                    hostService.sendACK(connection, BrokerConstants.REQUESTER.BROKER);
                    dataTransfer.setMethod(BrokerConstants.METHOD.SYNC);
                    dataTransfer.setReceiver(request.getRequest().getReceiver());
                    dataTransfer.processRequest(topicReadWriteRequest);
                } else {
                    logger.warn(String.format("[%s] Broker don't handle the topic %s-%d. Sending NACK for the PULL request from the broker", CacheManager.getBrokerInfo().getString(), topicReadWriteRequest.getName(), topicReadWriteRequest.getPartition()));
                    hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER);
                }
            } else {
                logger.info(String.format("[%s] Received invalid PULL request", CacheManager.getBrokerInfo().getString()));
            }
        }
    }

    /**
     * Process requests from broker with header type as DATA (Receive data, etc.)
     */
    private void processDataRequest(byte[] message) {
        byte[] data = BrokerPacketHandler.getData(message);

        if (data != null) {
            DataPacket dataPacket = JSONDesrializer.fromJson(data, DataPacket.class);

            if (dataPacket != null && dataPacket.isValid()) {
                logger.info(String.format("[%s] Received DATA packet [%s] for the partition key %s from another broker", CacheManager.getBrokerInfo().getString(), dataPacket.getDataType(), dataPacket.getKey()));
                File partition = CacheManager.getPartition(dataPacket.getKey());

                if (partition != null) {
                    boolean isSuccess = partition.write(dataPacket);

                    if (isSuccess) {
                        logger.info(String.format("[%s] Sending ACK to the broker as written %s type data to local for the partition %s", CacheManager.getBrokerInfo().getString(), dataPacket.getDataType(), dataPacket.getKey()));
                        hostService.sendACK(connection, BrokerConstants.REQUESTER.BROKER);

                        if (toFlushBuffer(dataPacket, partition)) {
                            //flush to segment if received the data till snapshot taken from broker
                            partition.flushToSegment();

                            if (CacheManager.isLeader(dataPacket.getKey(), CacheManager.getBrokerInfo())) {
                                logger.info(String.format("[%s] Checking some of the followers the logs if they are behind the leader", CacheManager.getBrokerInfo().getString()));
                                sendDataToOutdatedBrokers(dataPacket, partition);
                            }

                            logger.info(String.format("[%s] Received all the data from the snapshot taken with the leader. Flush the local buffer to the segment. Changing broker status to READY", CacheManager.getBrokerInfo().getString()));
                            CacheManager.setStatus(dataPacket.getKey(), BrokerConstants.BROKER_STATE.READY);
                        }

                        CacheManager.updatePartition(dataPacket.getKey(), partition);
                    } else {
                        logger.info(String.format("[%s] Unable to write the received data. Sending NACK", CacheManager.getBrokerInfo().getString()));
                        hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER);
                    }
                } else {
                    logger.info(String.format("[%s] Unable to write the received data as broker not handling the partition %s. Sending NACK", CacheManager.getBrokerInfo().getString(), dataPacket.getKey()));
                    hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER);
                }
            } else {
                logger.warn(String.format("[%s] Received invalid request from another broker.", CacheManager.getBrokerInfo().getString()));
            }
        } else {
            logger.warn(String.format("[%s] Received no data from another broker.", CacheManager.getBrokerInfo().getString()));
        }
    }

    /**
     * Get the list of brokers which are out of sync for the given partition
     */
    private List<Broker> getOutOfSyncBrokers(String key) {
        List<Broker> outOfSyncBrokers;

        Brokers brokers = CacheManager.getBrokers(key);
        outOfSyncBrokers = brokers.getOutOfSyncBrokers();


        return outOfSyncBrokers;
    }

    /**
     * Check for a need to flush local buffer if maintained to segment
     */
    private boolean toFlushBuffer(DataPacket dataPacket, File partition) {
        BrokerConstants.BROKER_STATE broker_state = CacheManager.getStatus(dataPacket.getKey());

        return broker_state == BrokerConstants.BROKER_STATE.SYNC && Objects.equals(dataPacket.getDataType(), BrokerConstants.DATA_TYPE.CATCH_UP_DATA) && partition.getOffset() == dataPacket.getToOffset();
    }

    /**
     * Check and send up-to-date brokers which are lagging behind
     */
    private void sendDataToOutdatedBrokers(DataPacket dataPacket, File partition) {
        List<Broker> outOfSyncBrokers = getOutOfSyncBrokers(dataPacket.getKey());

        for (Broker broker : outOfSyncBrokers) {
            String[] parts = partition.getName().split(":");
            TopicReadWriteRequest topicReadWriteRequest = new TopicReadWriteRequest(parts[0], Integer.parseInt(parts[1]), broker.getNextOffset(), partition.getOffset());
            dataTransfer.setMethod(BrokerConstants.METHOD.SYNC);
            dataTransfer.processRequest(topicReadWriteRequest);

            broker.setOutOfSync(false);
            broker.setNextOffsetToRead(0);
        }
    }
}
