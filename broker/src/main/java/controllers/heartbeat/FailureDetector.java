package controllers.heartbeat;

import configurations.BrokerConstants;
import models.HeartBeatReceivedTime;
import models.HeartBeatReceivedTimes;

/**
 * Responsible for detecting whether the broker holding the particular partition crashed.
 *
 * @author Palak Jain
 */
public class FailureDetector {
    private HeartBeatReceivedTimes heartBeatReceivedTimes;

    public FailureDetector() {
        heartBeatReceivedTimes = new HeartBeatReceivedTimes();
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
            if (receivedTime.isMaxRetry()) {
                //markDown(receivedTime.getKey(), receivedTime.getServerId());
            } else {
                receivedTime.incrementRetry();
            }
        } else {
            receivedTime.resetRetryCount();
        }
    }

    /**
     * Get the task name
     */
    private String getTaskName(String key, String serverId) {
        return String.format("HeartBeat_%s_%s", key, serverId);
    }
}
