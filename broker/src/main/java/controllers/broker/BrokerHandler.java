package controllers.broker;

import configurations.BrokerConstants;
import controllers.*;
import controllers.database.CacheManager;
import controllers.election.Election;
import controllers.heartbeat.HeartBeatReceiver;
import controllers.heartbeat.HeartBeatSender;
import controllers.replication.SyncManager;
import models.Header;
import models.data.File;
import models.heartbeat.HeartBeatRequest;
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
            //TODO: Log
            HeartBeatReceiver heartBeatReceiver = new HeartBeatReceiver();
            heartBeatReceiver.handleRequest(connection, message);
        } else if (header.getType() == BrokerConstants.TYPE.ELECTION.getValue()) {
            //TODO: Log
            hostService.sendACK(connection, BrokerConstants.REQUESTER.BROKER);

            Election election = new Election();
            election.handleRequest(message);
        } else if (header.getType() == BrokerConstants.TYPE.UPDATE.getValue()) {
            //TODO: Log
            processUpdateRequest(message);
        } else if (header.getType() == BrokerConstants.TYPE.REQ.getValue()) {
            //TODO: log
            processREQRequest(message);
        } else if (header.getType() == BrokerConstants.TYPE.PULL.getValue()) {
            //TODO: log
            processPullRequest(message);
        } else if (header.getType() == BrokerConstants.TYPE.DATA.getValue()) {
            //TODO: log
            processDataRequest(message);
        }
    }

    /**
     * Process requests from broker with header type as Update (Updating leader, etc.)
     */
    private void processUpdateRequest(byte[] message) {
        byte[] data = BrokerPacketHandler.getData(message);

        if (data != null) {
            Request<BrokerUpdateRequest> brokerUpdateRequest = JSONDesrializer.deserializeRequest(data, BrokerUpdateRequest.class);

            if (brokerUpdateRequest != null && brokerUpdateRequest.getRequest() != null) {
                if (Objects.equals(brokerUpdateRequest.getType(), BrokerConstants.REQUEST_TYPE.LEADER)) {
                    //TODO: Log
                    CacheManager.setLeader(brokerUpdateRequest.getRequest().getKey(), new Broker(brokerUpdateRequest.getRequest().getBroker()));
                    hostService.sendACK(connection, BrokerConstants.REQUESTER.BROKER);

                    //Start sending heartbeat messages to another brokers
                    //TODO: log
                    HeartBeatRequest heartBeatRequest = new HeartBeatRequest(brokerUpdateRequest.getRequest().getKey(), CacheManager.getBrokerInfo().getString(), brokerUpdateRequest.getRequest().getBroker().getString());
                    heartBeatSender.send(heartBeatRequest);
                    syncManager.sync(brokerUpdateRequest.getRequest().getKey());
                } else {
                    //TODO: log
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
            Request<OffsetRequest> request = JSONDesrializer.deserializeRequest(data, OffsetRequest.class);

            if (request != null && request.getRequest() != null && request.getRequest().isValid()) {
                if (request.getType().equalsIgnoreCase(BrokerConstants.REQUEST_TYPE.OFFSET)) {
                    //TODO: log
                    OffsetRequest offsetRequest = request.getRequest();

                    File partition = CacheManager.getPartition(offsetRequest.getKey());

                    if (partition != null) {
                        byte[] response = BrokerPacketHandler.createOffsetResponse(offsetRequest.getKey(), partition.getOffset(), partition.getTotalSize());
                        connection.send(response);
                        //TODO: log
                    } else {
                        //TODO: log
                        hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER);
                    }
                } else {
                    //TODO: log
                }
            } else {
                //TODO: log
            }
        }
    }

    /**
     * Process requests from broker with header type as PULL (Send data from given offset, etc.)
     */
    private void processPullRequest(byte[] message) {
        byte[] data = BrokerPacketHandler.getData(message);

        if (data != null) {
            Request<TopicReadWriteRequest> request = JSONDesrializer.deserializeRequest(data, TopicReadWriteRequest.class);

            if (request != null && request.getRequest() != null && request.getRequest().isValid()) {
                //TODO: log
                TopicReadWriteRequest topicReadWriteRequest = request.getRequest();
                dataTransfer.setMethod(BrokerConstants.METHOD.SYNC);
                dataTransfer.processRequest(topicReadWriteRequest);
            } else {
                //TODO: log
            }
        }
    }

    /**
     * Process requests from broker with header type as DATA (Receive data, etc.)
     */
    private void processDataRequest(byte[] message) {
        byte[] data = BrokerPacketHandler.getData(message);

        if (data != null) {
            Request<DataPacket> request = JSONDesrializer.deserializeRequest(data, DataPacket.class);

            if (request != null && request.getRequest() != null && request.getRequest().isValid()) {
                //TODO: log
                DataPacket dataPacket = request.getRequest();
                File partition = CacheManager.getPartition(dataPacket.getKey());

                if (partition != null) {
                    boolean isSuccess = partition.write(dataPacket);

                    if (isSuccess) {
                        //TODO: log
                        hostService.sendACK(connection, BrokerConstants.REQUESTER.BROKER);

                        if (toFlushBuffer(dataPacket, partition)) {
                            //flush to segment if received the data till snapshot taken from broker
                            partition.flushToSegment();

                            sendDataToOutdatedBrokers(dataPacket, partition);

                            CacheManager.setStatus(dataPacket.getKey(), BrokerConstants.BROKER_STATE.READY);
                        }
                    } else {
                        //TODO: log
                        hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER);
                    }
                } else {
                    //TODO: log
                    hostService.sendNACK(connection, BrokerConstants.REQUESTER.BROKER);
                }
            } else {
                //TODO: log
            }
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
