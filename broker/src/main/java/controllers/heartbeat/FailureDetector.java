package controllers.heartbeat;

import configurations.BrokerConstants;
import controllers.Broker;
import controllers.Channels;
import controllers.database.CacheManager;
import controllers.loadBalancer.LBHandler;
import models.heartbeat.HeartBeatReceivedTime;
import models.heartbeat.HeartBeatReceivedTimes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.BrokerPacketHandler;

/**
 * Responsible for detecting whether the broker holding the particular partition crashed.
 *
 * @author Palak Jain
 */
public class FailureDetector {
    private static final Logger logger = LogManager.getLogger(FailureDetector.class);
    private HeartBeatReceivedTimes heartBeatReceivedTimes;
    private LBHandler lbHandler;

    public FailureDetector() {
        heartBeatReceivedTimes = new HeartBeatReceivedTimes();
        lbHandler = new LBHandler();
    }

    /**
     * Update the received timespan for the given server holding particular partition
     */
    public synchronized void heartBeatReceived(String key, String serverId) {
        long currentTime = System.nanoTime();

        HeartBeatReceivedTime heartBeatReceivedTime = heartBeatReceivedTimes.get(key, serverId);

        if (heartBeatReceivedTime == null) {
            heartBeatReceivedTime = new HeartBeatReceivedTime(key, serverId);
            heartBeatReceivedTimes.add(key, serverId);

            start(key, serverId);
        }

        heartBeatReceivedTime.setTimespan(currentTime);
        logger.info(String.format("[%s] Received heartbeat message from the broker %s for key %s at timer %d", CacheManager.getBrokerInfo().getString(), serverId, key, currentTime));
        System.out.printf("[%s] Received heartbeat message from the broker %s for key %s at timer %d%n", CacheManager.getBrokerInfo().getString(), serverId, key, currentTime);
    }

    /**
     * Marking the given broker as failed
     */
    public void markDown(String key, String serverId) {
        Broker broker = CacheManager.getBroker(key, serverId);
        logger.info(String.format("[%s] Marking %s broker down for the partition %s.", CacheManager.getBrokerInfo().getString(), serverId, key));

        //Remove the broker from the list of brokers handling the partition of the topic
        CacheManager.removeBroker(key, broker);

        //Set the status of the current broker as "waiting for new follower"
        CacheManager.setStatus(key, BrokerConstants.BROKER_STATE.WAIT_FOR_NEW_FOLLOWER);
        logger.info(String.format("[%s] Changing the status of broker to WAIT FOR NEW FOLLOWER when %s broker failed for the partition %s.", CacheManager.getBrokerInfo().getString(), serverId, key));

        //Checking if the current broker is leader of the topic partition
        if(CacheManager.isLeader(key, broker)) {
            CacheManager.setLeaderAsInActive(key);
            broker.setDesignation(BrokerConstants.BROKER_DESIGNATION.LEADER.getValue());
        } else {
            broker.setDesignation(BrokerConstants.BROKER_DESIGNATION.FOLLOWER.getValue());
        }

        //Letting load balancer know about it
        byte[] packet = BrokerPacketHandler.createFailBrokerPacket(key, broker);
        lbHandler.sendLeaderUpdate(packet);
        logger.info(String.format("[%s] Send broker failure notification to load balancer.", CacheManager.getBrokerInfo().getString()));

        HeartBeatSchedular.cancel(getTaskName(key, serverId));

        //Close down connection
        Channels.remove(serverId);
    }

    /**
     * Start the scheduler to run the check on received heartbeat messages
     */
    private void start(String key, String serverId) {
        HeartBeatSchedular.start(getTaskName(key, serverId), ()-> heartBeatCheck(key, serverId), BrokerConstants.HEARTBEAT_CHECK_TIME);
    }

    /**
     * Check whether the timespan of the given server since last time exceeded the threshold wait time.
     */
    private void heartBeatCheck(String key, String serverId) {
        long now = System.nanoTime();

        HeartBeatReceivedTime receivedTime = heartBeatReceivedTimes.get(key, serverId);

        long lastHeartBeatReceivedTime = receivedTime.getTimespan();
        long timeSinceLastHeartBeat = now - lastHeartBeatReceivedTime;

        if (timeSinceLastHeartBeat >= BrokerConstants.HEARTBEAT_TIMEOUT_THRESHOLD) {
            logger.warn(String.format("[%s] Marking broker %s failed as the time since last heart beat %d exceed %d threshold.", CacheManager.getBrokerInfo().getString(), receivedTime.getServerId(), timeSinceLastHeartBeat, BrokerConstants.HEARTBEAT_TIMEOUT_THRESHOLD));
            markDown(receivedTime.getKey(), receivedTime.getServerId());
        }
    }

    /**
     * Get the task name
     */
    private String getTaskName(String key, String serverId) {
        return String.format("HeartBeat_%s_%s", key, serverId);
    }
}
