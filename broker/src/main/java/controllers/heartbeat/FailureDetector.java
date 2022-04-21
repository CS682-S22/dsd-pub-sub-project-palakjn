package controllers.heartbeat;

import configurations.BrokerConstants;
import controllers.Broker;
import controllers.Channels;
import controllers.Connection;
import controllers.HostService;
import controllers.database.CacheManager;
import models.heartbeat.HeartBeatReceivedTime;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.BrokerPacketHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Responsible for detecting whether the broker holding the particular partition crashed.
 *
 * @author Palak Jain
 */
public class FailureDetector {
    private static final Logger logger = LogManager.getLogger(FailureDetector.class);
    private HostService hostService;
    private static Map<String, Timer> timerTaskMap = new HashMap<>();

    public FailureDetector() {
        hostService = new HostService(logger);
    }

    /**
     * Update the received timespan for the given server holding particular partition
     */
    public synchronized void heartBeatReceived(String key, String serverId) {
        long currentTime = System.currentTimeMillis();

        HeartBeatReceivedTime heartBeatReceivedTime = CacheManager.getHeartBeatReceivedTime(key, serverId);

        if (heartBeatReceivedTime == null) {
            heartBeatReceivedTime = new HeartBeatReceivedTime(key, serverId);
            CacheManager.createHeartBeatObject(key, serverId);

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
        logger.info(String.format("[%s] Marking %s broker down for the partition %s.", CacheManager.getBrokerInfo().getString(), serverId, key));

        if (CacheManager.changeStatusToWaitForFollower(key, serverId)) {
            Broker broker = CacheManager.getBroker(key, serverId);

            if (broker != null) {
                //Checking if the current broker is leader of the topic partition
                if(CacheManager.isLeader(key, broker)) {
                    CacheManager.setLeaderAsInActive(key);
                    broker.setDesignation(BrokerConstants.BROKER_DESIGNATION.LEADER.getValue());
                } else {
                    broker.setDesignation(BrokerConstants.BROKER_DESIGNATION.FOLLOWER.getValue());
                }

                //Letting load balancer know about it
                byte[] packet = BrokerPacketHandler.createFailBrokerPacket(key, broker);
                sendLeaderUpdate(packet);
                logger.info(String.format("[%s] Send broker failure notification to load balancer.", CacheManager.getBrokerInfo().getString()));
            }
        }

        Timer timer = timerTaskMap.getOrDefault(getTaskName(key, serverId), null);

        if (timer != null) {
            timer.cancel();
            timerTaskMap.remove(getTaskName(key, serverId));
        }

        //Close down connection
        Channels.remove(serverId);
    }

    /**
     * Send new leader information to load balancer
     */
    public void sendLeaderUpdate(byte[] packet) {
        //Get the connection with load balancer
        Connection connection = connectToLoadBalancer();

        if (connection != null) {
            //Send leader info and wait for an ack
            hostService.sendPacketWithACK(connection, packet, BrokerConstants.ACK_WAIT_TIME, true);
        }
    }

    /**
     * Connect with the load balancer.
     */
    private Connection connectToLoadBalancer() {
       Connection connection = Channels.get(null, BrokerConstants.CHANNEL_TYPE.LOADBALANCER);

        if (connection == null || !connection.isOpen()) {
            connection = hostService.connect(CacheManager.getLoadBalancer().getAddress(), CacheManager.getLoadBalancer().getPort());
        }

        if (connection != null && connection.isOpen()) {
            Channels.upsert(null, connection, BrokerConstants.CHANNEL_TYPE.LOADBALANCER);
        }

        return connection;
    }

    /**
     * Start the scheduler to run the check on received heartbeat messages
     */
    private void start(String key, String serverId) {
        Timer timer = timerTaskMap.getOrDefault(getTaskName(key, serverId), new Timer());

        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                heartBeatCheck(key, serverId);
                this.cancel();
            }
        };

        timer.schedule(timerTask, BrokerConstants.HEARTBEAT_CHECK_TIME);

        timerTaskMap.put(getTaskName(key, serverId), timer);
    }

    /**
     * Check whether the timespan of the given server since last time exceeded the threshold wait time.
     */
    private void heartBeatCheck(String key, String serverId) {
        long now = System.currentTimeMillis();

        HeartBeatReceivedTime receivedTime = CacheManager.getHeartBeatReceivedTime(key, serverId);

        long lastHeartBeatReceivedTime = receivedTime.getTimespan();
        long timeSinceLastHeartBeat = now - lastHeartBeatReceivedTime;

        if (timeSinceLastHeartBeat >= BrokerConstants.HEARTBEAT_TIMEOUT_THRESHOLD) {
            logger.warn(String.format("[%s] Marking broker %s failed as the time since last heart beat %d exceed %d threshold.", CacheManager.getBrokerInfo().getString(), receivedTime.getServerId(), timeSinceLastHeartBeat, BrokerConstants.HEARTBEAT_TIMEOUT_THRESHOLD));
            markDown(receivedTime.getKey(), receivedTime.getServerId());
        } else {
            Timer timer = timerTaskMap.getOrDefault(getTaskName(key, serverId), null);

            if (timer != null) {
                timer.cancel();
                timerTaskMap.remove(getTaskName(key, serverId));
            }

            start(key, serverId);
        }
    }

    /**
     * Get the task name
     */
    private String getTaskName(String key, String serverId) {
        return String.format("HeartBeat_%s_%s", key, serverId);
    }
}
