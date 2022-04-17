package controllers.election;

import configurations.BrokerConstants;
import controllers.database.CacheManager;
import controllers.heartbeat.FailureDetector;
import controllers.loadBalancer.LBHandler;
import controllers.replication.Broker;
import models.ElectionRequest;
import models.Host;
import models.requests.Request;
import utilities.BrokerPacketHandler;
import utilities.JSONDesrializer;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class Election {
    private Timer timer;
    private FailureDetector failureDetector;

    public Election() {
        failureDetector = new FailureDetector();
    }

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
                    timer = new Timer();
                    TimerTask task = new TimerTask() {
                        @Override
                        public void run() {
                            checkForLeaderUpdate(key);
                            timer.cancel();
                            this.cancel();
                        }
                    };
                    timer.schedule(task, BrokerConstants.ELECTION_RESPONSE_WAIT_TIME);
                } else {
                    //TODO: Log
                    failureDetector.markDown(key, highPriorityBroker.getString());
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

    public void handleRequest(byte[] message) {
        byte[] data = BrokerPacketHandler.getData(message);

        if (data != null) {
            Request<ElectionRequest> electionRequest = JSONDesrializer.fromJson(data, Request.class);

            if (electionRequest != null && electionRequest.getRequest() != null) {
                ElectionRequest request = electionRequest.getRequest();

                if (CacheManager.getStatus(request.getKey()) != BrokerConstants.BROKER_STATE.ELECTION) {
                    //TODO: Log
                    start(request.getKey());
                } else {
                    //TODO: Log
                }
            }
        } else {
            //TODO: log
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

    private void checkForLeaderUpdate(String key) {
        Host leader = CacheManager.getLeader(key);

        if (leader != null && leader.isActive()) {
            //TODO: log that new leader being elected
            CacheManager.setStatus(key, BrokerConstants.BROKER_STATE.READY);
        } else {
            //TODO: log that new leader not being decided yet. Starting election again
            start(key);
        }
    }
}
