package controllers.election;

import com.google.gson.reflect.TypeToken;
import configurations.BrokerConstants;
import controllers.database.CacheManager;
import controllers.heartbeat.FailureDetector;
import controllers.loadBalancer.LBHandler;
import controllers.Broker;
import controllers.replication.SyncManager;
import models.election.ElectionRequest;
import models.Host;
import models.requests.Request;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.BrokerPacketHandler;
import utilities.JSONDesrializer;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Responsible for conducting election to elect leader
 *
 * @author Palak Jain
 */
public class Election {
    private static final Logger logger = LogManager.getLogger(Election.class);
    private Timer timer;
    private FailureDetector failureDetector;
    private SyncManager syncManager;

    public Election() {
        failureDetector = new FailureDetector();
        syncManager = new SyncManager();
    }

    /**
     * Start the election process for the given key when the given leader is failed
     */
    public void start(String key, Host failedBroker) {
        BrokerConstants.BROKER_STATE broker_state = CacheManager.getStatus(key);

        if (broker_state != BrokerConstants.BROKER_STATE.ELECTION) {
            System.out.printf("[%s] Starting election for partition %s.\n", CacheManager.getBrokerInfo().getString(), key);

            //Change the broker state to "Election"
            CacheManager.setStatus(key, BrokerConstants.BROKER_STATE.ELECTION);

            //Getting brokers with higher priority than the current one
            List<Broker> highPriorityBrokers = CacheManager.getBrokers(key, BrokerConstants.PRIORITY_CHOICE.HIGH);

            boolean electItself = false;

            if (highPriorityBrokers != null && highPriorityBrokers.size() > 0) {
                //Sending "Election" message to those brokers
                logger.info(String.format("[%s:%d] Sending \"Election\" message to %d high priority brokers.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort(), highPriorityBrokers.size()));
                byte[] electionPacket = BrokerPacketHandler.createElectionPacket(key, failedBroker);

                boolean isSuccess = false;

                for (Broker highPriorityBroker : highPriorityBrokers) {
                    isSuccess = highPriorityBroker.send(electionPacket, BrokerConstants.CHANNEL_TYPE.HEARTBEAT, BrokerConstants.ELECTION_RESPONSE_WAIT_TIME, false) || isSuccess;

                    if (isSuccess) {
                        logger.info(String.format("[%s:%d] Send \"Election\" message to broker %s:%d.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort(), highPriorityBroker.getAddress(), highPriorityBroker.getPort()));
                    } else {
                        logger.warn(String.format("[%s:%d] Received no response from the broker %s:%d for \"Election\" message. Marking down the broker.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort(), highPriorityBroker.getAddress(), highPriorityBroker.getPort()));
                        failureDetector.markDown(key, highPriorityBroker.getString());
                    }
                }

                if (isSuccess) {
                    logger.info(String.format("[%s:%d] Received \"Election\" response from all other brokers. Waiting for leader update.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort()));
                    timer = new Timer();
                    TimerTask task = new TimerTask() {
                        @Override
                        public void run() {
                            checkForLeaderUpdate(key, failedBroker);
                            timer.cancel();
                            this.cancel();
                        }
                    };
                    timer.schedule(task, BrokerConstants.ELECTION_RESPONSE_WAIT_TIME);
                } else {
                    logger.info(String.format("[%s:%d] Not \"Election\" response from any of the brokers. Electing itself as leader.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort()));
                    electItself = true;
                }
            } else {
                electItself = true;

                //No brokers with high priority found
                logger.info(String.format("[%s:%d] No high priority brokers found. Electing itself as leader and sending leader update to low priority brokers.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort()));
            }

            if (electItself) {
                //Elect itself as leader
                CacheManager.setLeader(key, new Broker(CacheManager.getBrokerInfo()));

                //Sending "I am leader" message to other brokers and load balancer
                sendLeaderUpdate(key);

                System.out.printf("[%s] I am the new leader. Membership table for partition %s: %s.\n", CacheManager.getBrokerInfo().getString(), key, CacheManager.getMemberShipTable(key));

                //Set the broker instance to "Sync"
                syncManager.sync(key);
            }
        } else {
            logger.info(String.format("[%s] Election is in already progress for partition %s.", CacheManager.getBrokerInfo().getString(), key));
        }
    }

    /**
     * Handle election request received from another broker
     */
    public void handleRequest(byte[] message) {
        byte[] data = BrokerPacketHandler.getData(message);

        if (data != null) {
            Request<ElectionRequest> electionRequest = JSONDesrializer.deserializeRequest(data, new TypeToken<Request<ElectionRequest>>(){}.getType());

            if (electionRequest != null && electionRequest.getRequest() != null) {
                ElectionRequest request = electionRequest.getRequest();

                if (CacheManager.getStatus(request.getKey()) != BrokerConstants.BROKER_STATE.ELECTION) {
                    if (CacheManager.isLeader(request.getKey(), request.getFailedBroker())) {
                        logger.info(String.format("[%s:%d] Received \"Election\" message and leader %s:%d failed. Starting election.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort(), request.getFailedBroker().getAddress(), request.getFailedBroker().getPort()));
                        CacheManager.removeBroker(request.getKey(), request.getFailedBroker());
                        start(request.getKey(), request.getFailedBroker());
                    } else {
                        logger.info(String.format("[%s:%d] Received \"Election\" message. Election already happen. Not starting new election for failed leader %s:%d", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort(), request.getFailedBroker().getAddress(), request.getFailedBroker().getPort()));
                    }
                } else {
                    logger.info(String.format("[%s:%d] Received \"Election\" message. Broker is already in election mode. Not starting new election.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort()));
                }
            }
        } else {
            logger.warn(String.format("[%s:%d] Unable to read \"Election\" message from the request body", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort()));
        }
    }

    /**
     * Send new leader information to all other brokers handling the given partition and also to the load balancer
     */
    private void sendLeaderUpdate(String key) {
        byte[] packet = BrokerPacketHandler.createLeaderUpdateRequest(key, CacheManager.getBrokerInfo());

        //Sending new leader update to low priority brokers
        List<Broker> lowPriorityBrokers = CacheManager.getBrokers(key, BrokerConstants.PRIORITY_CHOICE.LESS);

        if (lowPriorityBrokers != null) {
            for (Broker broker : lowPriorityBrokers) {
                broker.send(packet, BrokerConstants.CHANNEL_TYPE.HEARTBEAT, BrokerConstants.ACK_WAIT_TIME, true);
                logger.info(String.format("[%s:%d] Send \"I am leader \" update to broker %s:%d.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort(), broker.getAddress(), broker.getPort()));
            }
        }

        //Sending to load balancer
        LBHandler handler = new LBHandler();
        handler.sendLeaderUpdate(packet);
        logger.info(String.format("[%s:%d] Send \"I am leader \" update to load balancer.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort()));
    }

    /**
     * Check for victory update i.e. new leader update received from other broker
     */
    private void checkForLeaderUpdate(String key, Host failedBroker) {
        Host leader = CacheManager.getLeader(key);

        if (leader != null && leader.isActive()) {
            logger.info(String.format("[%s:%d] New leader %s:%d is being elected for the partition %s.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort(), leader.getAddress(), leader.getPort(), key));
            System.out.printf("[%s] Broker %s become the leader.\n", CacheManager.getBrokerInfo().getString(), leader.getString());
            syncManager.sync(key);
        } else {
            logger.warn(String.format("[%s:%d] No new leader being chosen for the topic %s. Starting election again.", CacheManager.getBrokerInfo().getAddress(), CacheManager.getBrokerInfo().getPort(), key));
            start(key, failedBroker);
        }
    }
}
