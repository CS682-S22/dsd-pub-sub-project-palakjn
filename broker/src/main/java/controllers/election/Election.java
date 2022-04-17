package controllers.election;

import configurations.BrokerConstants;
import controllers.database.CacheManager;
import controllers.loadBalancer.LBHandler;
import controllers.replication.Broker;
import utilities.BrokerPacketHandler;

import java.util.List;

public class Election {

    public void start(String key) {
        //Change the broker state to "Election"
        CacheManager.setStatus(key, BrokerConstants.BROKER_STATE.ELECTION);

        //Getting brokers with higher priority than the current one
        List<Broker> highPriorityBrokers = CacheManager.getBrokers(key, BrokerConstants.PRIORITY_CHOICE.HIGH);

        if (highPriorityBrokers != null) {
            //Sending "Election" message to those brokers
            byte[] electionPacket = BrokerPacketHandler.createElectionPacket(key);

            for (Broker highPriorityBroker : highPriorityBrokers) {
                boolean isSuccess = highPriorityBroker.send(electionPacket, BrokerConstants.CHANNEL_TYPE.HEARTBEAT, BrokerConstants.ELECTION_RESPONSE_WAIT_TIME, false);

                if (isSuccess) {
                    //TODO: Log
                    //TODO: run the node timer to check if leader information received or not. If not within the timer then, start election again
                } else {
                    //TODO: faultDetector.markDown(key, highPriorityBroker);
                }
            }
        } else {
            //No brokers with high priority found

            //Elect itself as leader
            CacheManager.setLeader(key, new Broker(CacheManager.getBrokerInfo()));

            //Sending "I am leader" message to other brokers and load balancer
            sendLeaderUpdate(key);

            //Set the broker instance to "Sync"
            CacheManager.setStatus(key, BrokerConstants.BROKER_STATE.SYNC);

            //TODO: Call sync module
        }
    }

    private void sendLeaderUpdate(String key) {
        byte[] packet = BrokerPacketHandler.createLeaderUpdateRequest(key, CacheManager.getBrokerInfo());

        //Sending new leader update to low priority brokers
        List<Broker> lowPriorityBrokers = CacheManager.getBrokers(key, BrokerConstants.PRIORITY_CHOICE.LESS);

        if (lowPriorityBrokers != null) {
            for (Broker broker : lowPriorityBrokers) {
                broker.send(packet, BrokerConstants.CHANNEL_TYPE.HEARTBEAT, BrokerConstants.ACK_WAIT_TIME, true);
            }
        }

        //Sending to load balancer
        LBHandler handler = new LBHandler();
        handler.sendLeaderUpdate(packet);
    }
}
