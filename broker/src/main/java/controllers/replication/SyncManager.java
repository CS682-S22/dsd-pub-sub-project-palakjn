package controllers.replication;

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
        //Set the status of the broker as sync
        CacheManager.setStatus(key, BrokerConstants.BROKER_STATE.SYNC);

        //Get whether the broker is leader of the given partition key
        boolean isLeader = CacheManager.isLeader(key, CacheManager.getBrokerInfo());

        if (isLeader) {
            //If leader then, send the request to followers to ask for the "offset"
            syncLeader(key);
        } else {
            //If follower then, send the request to leader to ask for the "offset"
            syncFollower(key);
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

        if (brokers != null) {
            List<Broker> brokerList = brokers.getBrokers();

            List<OffsetResponse> offsetResponseList = new ArrayList<>();

            for (Broker broker : brokerList) {
                //Sending the offset request to the broker
                byte[] response =  broker.sendAndGetResponse(packet, BrokerConstants.CHANNEL_TYPE.DATA, BrokerConstants.ACK_WAIT_TIME, true);

                if (response != null) {
                    Response<OffsetResponse> offsetResponse = JSONDesrializer.deserializeResponse(response, OffsetResponse.class);

                    if (offsetResponse != null && offsetResponse.isValid() && offsetResponse.isOk()) {
                        offsetResponse.getObject().setBrokerInfo(broker);
                        offsetResponseList.add(offsetResponse.getObject());
                    }
                }
            }

            //Finding offset response from the broker from whom the current broker is far behind
            OffsetResponse offsetResponse = getLargeOffsetResponse(offsetResponseList);

            sendRequestToBroker(offsetResponse);
        } else {
            //TODO: log
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
                response.getBrokerInfo().setOutOfSync(true);
                response.getBrokerInfo().setNextOffsetToRead(response.getSize());

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
            byte[] response =  leader.sendAndGetResponse(packet, BrokerConstants.CHANNEL_TYPE.DATA, BrokerConstants.ACK_WAIT_TIME, true);

            if (response != null) {
                Response<OffsetResponse> offsetResponse = JSONDesrializer.deserializeResponse(response, OffsetResponse.class);

                if (offsetResponse != null && offsetResponse.isValid() && offsetResponse.isOk()) {
                    offsetResponse.getObject().setBrokerInfo(leader);
                    sendRequestToBroker(offsetResponse.getObject());
                } else {
                    //TODO: log
                }
            } else {
                //TODO: log
            }
        }
    }

    /**
     * Ask the broker to get the data from the given offset if the current broker is behind
     */
    private void sendRequestToBroker(OffsetResponse offsetResponse) {
        //TODO: log
        File partition = CacheManager.getPartition(offsetResponse.getKey());

        if (partition != null) {
            if (offsetResponse.getOffset() > partition.getOffset()) {
                //If received offset is more than the have offset for the partition then, sending request to Broker to send the data
                byte[] dataRequest = BrokerPacketHandler.createGetDataRequest(offsetResponse.getKey(), partition.getTotalSize(), offsetResponse.getOffset());

                //Sending the data request to the broker
                offsetResponse.getBrokerInfo().send(dataRequest, BrokerConstants.CHANNEL_TYPE.DATA, BrokerConstants.ACK_WAIT_TIME, true);
                //TODO: log
            } else {
                //TODO: log
                CacheManager.setStatus(offsetResponse.getKey(), BrokerConstants.BROKER_STATE.READY);
            }
        }
    }
}
