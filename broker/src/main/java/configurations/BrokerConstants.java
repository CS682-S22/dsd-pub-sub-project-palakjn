package configurations;

import configuration.Constants;

/**
 * Responsible for declaring constants used by Broker
 *
 * @author Palak Jain
 */
public class BrokerConstants extends Constants {
    public static String TOPIC_LOCATION = "data/%s_%s";
    public static int MAX_SEGMENT_MESSAGES = 500;
    public static long SEGMENT_FLUSH_TIME = 5000; //5 seconds
    public static int HEARTBEAT_INTERVAL_MS = 1000; //1 seconds
    public static int HEARTBEAT_CHECK_TIME = 5000; //5 seconds
    public static int HEARTBEAT_MAX_RETRY = 3;
    public static int HEARTBEAT_TIMEOUT_THRESHOLD = 10000; //10 seconds
    public static int ELECTION_RESPONSE_WAIT_TIME = 30000; //30 seconds

    public enum BROKER_STATE {
        ELECTION,
        SYNC,
        READY,
        WAIT_FOR_NEW_FOLLOWER,
        NONE
    }

    public enum PRIORITY_CHOICE {
        HIGH,
        LESS
    }

    public static class DATA_TYPE {
        public static String CATCH_UP_DATA = "Catch up data";
        public static String REPLICA_DATA = "Replica data";
    }
}
