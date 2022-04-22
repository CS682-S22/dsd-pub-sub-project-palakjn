package controllers.replication;

import com.google.gson.reflect.TypeToken;
import configurations.BrokerConstants;
import controllers.Broker;
import controllers.Brokers;
import controllers.HostService;
import controllers.database.CacheManager;
import models.data.File;
import models.responses.Response;
import models.sync.OffsetResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.BrokerPacketHandler;
import utilities.JSONDesrializer;

import java.util.ArrayList;
import java.util.List;

/**
 * Responsible for sync the data with other brokers.
 *
 * @author Palak Jain
 */
public class SyncManager {
    private static final Logger logger = LogManager.getLogger(SyncManager.class);
    private HostService hostService;

    public SyncManager() {
        hostService = new HostService(logger);
    }

    /**
     * Sync the data with other brokers
     */
    public void sync(String key) {
        BrokerConstants.BROKER_STATE broker_state = CacheManager.getStatus(key);

        if (broker_state != BrokerConstants.BROKER_STATE.SYNC) {
            logger.info(String.format("[%s] Starting sync process for the partition %s", CacheManager.getBrokerInfo().getString(), key));
            //Set the status of the broker as sync
            CacheManager.setStatus(key, BrokerConstants.BROKER_STATE.SYNC);

            //Get whether the broker is leader of the given partition key
            boolean isLeader = CacheManager.isLeader(key, CacheManager.getBrokerInfo());

            if (isLeader) {
                //If leader then, send the request to followers to ask for the "offset"
                logger.info(String.format("[%s] [Leader] Starting sync data with other followers for the partition key %s", CacheManager.getBrokerInfo().getString(), key));
                syncLeader(key);
            } else {
                //If follower then, send the request to leader to ask for the "offset"
                logger.info(String.format("[%s] [Follower] Starting sync data with leader for the partition key %s", CacheManager.getBrokerInfo().getString(), key));
                syncFollower(key);
            }
        }
    }

    /**
     * Sync the data with followers if the current broker is leader for the partition
     */
    private void syncLeader(String key) {
        //Creating request packet
        byte[] packet = BrokerPacketHandler.createOffsetRequest(key);

        //Get the followers holding the partition key
        Brokers brokers = CacheManager.getBrokers(key);

        if (brokers != null && brokers.getSize() > 0) {
            List<Broker> brokerList = brokers.getBrokers();

            List<OffsetResponse> offsetResponseList = new ArrayList<>();

            for (Broker broker : brokerList) {
                //Sending the offset request to the broker
                logger.info(String.format("[%s] Sending request to follower %s to ask for the offset for the partition %s.", CacheManager.getBrokerInfo().getString(), broker.getString(), key));
                byte[] response =  broker.sendAndGetResponse(packet, BrokerConstants.CHANNEL_TYPE.DATA, BrokerConstants.ACK_WAIT_TIME, true);

                if (response != null) {
                    Response<OffsetResponse> offsetResponse = JSONDesrializer.deserializeResponse(response, new TypeToken<Response<OffsetResponse>>(){}.getType());

                    if (offsetResponse != null && offsetResponse.isValid() && offsetResponse.isOk()) {
                        logger.info(String.format("[%s] Received offset response from broker %s. Offset is %d for the key %s", CacheManager.getBrokerInfo().getString(), broker.getString(), offsetResponse.getObject().getOffset(), key));
                        offsetResponse.getObject().setBrokerInfo(broker);
                        offsetResponseList.add(offsetResponse.getObject());
                    }
                }
            }

            //Finding offset response from the broker from whom the current broker is far behind
            OffsetResponse offsetResponse = getLargeOffsetResponse(offsetResponseList);

            sendRequestToBroker(offsetResponse);
        } else {
            logger.warn(String.format("[%s] No brokers found for the key: %s", CacheManager.getBrokerInfo().getString(), key));
        }
    }

    /**
     * Filter the list of offset to find broker with max offset value
     */
    private OffsetResponse getLargeOffsetResponse(List<OffsetResponse> offsetResponseList) {
        OffsetResponse offsetResponse;

        offsetResponse = offsetResponseList.get(0);

        for (OffsetResponse response : offsetResponseList) {
            if (response.getOffset() > offsetResponse.getOffset()) {
                //Keeping the information that broker is out of sync, leader will update those brokers after getting up-to-date data
                CacheManager.setBrokerAsOutOfSync(response.getKey(), response.getBrokerInfo(), response.getSize());

                offsetResponse = response;
            }
        }

        return offsetResponse;
    }

    /**
     * Sync the data with leader if the current broker is follower for the partition
     */
    private void syncFollower(String key) {
        //Creating request packet
        byte[] packet = BrokerPacketHandler.createOffsetRequest(key);

        //Get the leader of the partition
        Broker leader = CacheManager.getLeader(key);

        //Send the request if the leader is active (in between, leader might have failed)
        if (leader != null && leader.isActive()) {
            logger.info(String.format("[%s] Sending request to leader %s to ask for the offset for the partition %s.", CacheManager.getBrokerInfo().getString(), leader.getString(), key));
            byte[] response =  leader.sendAndGetResponse(packet, BrokerConstants.CHANNEL_TYPE.DATA, BrokerConstants.ACK_WAIT_TIME, true);

            if (response != null) {
                Response<OffsetResponse> offsetResponse = JSONDesrializer.deserializeResponse(response, new TypeToken<Response<OffsetResponse>>(){}.getType());

                if (offsetResponse != null && offsetResponse.isValid() && offsetResponse.isOk()) {
                    logger.info(String.format("[%s] Received offset response from leader %s. Offset is %d for the key %s", CacheManager.getBrokerInfo().getString(), leader.getString(), offsetResponse.getObject().getOffset(), key));
                    offsetResponse.getObject().setBrokerInfo(leader);
                    sendRequestToBroker(offsetResponse.getObject());
                } else {
                    logger.warn(String.format("[%s] Received invalid offset response packet from other broker.", CacheManager.getBrokerInfo().getString()));
                }
            } else {
                logger.warn(String.format("[%s] Received invalid packet from other broker.", CacheManager.getBrokerInfo().getString()));
            }
        }
    }

    /**
     * Ask the broker to get the data from the given offset if the current broker is behind
     */
    private void sendRequestToBroker(OffsetResponse offsetResponse) {
        File partition = CacheManager.getPartition(offsetResponse.getKey());

        if (partition != null) {
            if (offsetResponse.getOffset() > partition.getOffset()) {
                //If received offset is more than the have offset for the partition then, sending request to Broker to send the data
                logger.info(String.format("[%s] Broker lag behind the broker %s for the key %s. Have Offset: %d. Expected Offset %d.", CacheManager.getBrokerInfo().getString(), offsetResponse.getBrokerInfo().getString(), offsetResponse.getKey(), partition.getOffset(), offsetResponse.getOffset()));
                byte[] dataRequest = BrokerPacketHandler.createGetDataRequest(offsetResponse.getKey(), CacheManager.getBrokerInfo().getString(), partition.getTotalSize(), offsetResponse.getOffset());

                //Sending the data request to the broker
                offsetResponse.getBrokerInfo().send(dataRequest, BrokerConstants.CHANNEL_TYPE.DATA, BrokerConstants.ACK_WAIT_TIME, true);
                logger.info(String.format("[%s] Send the request to get data for partition %s from offset %d to %d.", CacheManager.getBrokerInfo().getString(), offsetResponse.getKey(), partition.getTotalSize(), offsetResponse.getOffset()));
            } else {
                logger.info(String.format("[%s] Broker is up to date with the broker %s. Changing status to ready.", CacheManager.getBrokerInfo().getString(), offsetResponse.getBrokerInfo().getString()));
                CacheManager.setStatus(offsetResponse.getKey(), BrokerConstants.BROKER_STATE.READY);
            }
        }
    }
}
