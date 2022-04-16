package configuration;

import java.util.List;

/**
 * Responsible for holding constant values to use.
 *
 * @author Palak Jain
 */
public class Constants {
    public static final int START_VALID_PORT = 1700;
    public static final int END_VALID_PORT = 1724;
    public static int NUM_OF_THREADS = 50;
    public static int THREAD_COUNT = 1;
    public static int QUEUE_BUFFER_SIZE = 1000;
    public static int CONSUMER_WAIT_TIME = 10000;
    public static int CONSUMER_MAX_PULL_SIZE = 500;
    public static int PRODUCER_WAIT_TIME = 3000;
    public static int GET_BROKER_WAIT_TIME = 2000;
    public static int ACK_WAIT_TIME = 10000;
    public static int MAX_HEARTBEAT_TASKS =  100;

    public enum REQUESTER {
        LOAD_BALANCER(0),
        TOPIC(1),
        BROKER(2),
        PRODUCER(3),
        CONSUMER(4);

        private final int value;
        REQUESTER(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    public enum TYPE {
        SYN(0),
        REQ(1),  //Request
        RESP(2), //Response
        ACK(3),  //Acknowledgment
        NACK(4), //Negative Acknowledgement
        DATA(5),
        PULL(6),
        SUB(7),  //Subscribe
        FIN(8),
        UPDATE(9),
        HEARTBEAT(10),
        ELECTION(11);

        private final int value;
        TYPE(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    public static TYPE findTypeByValue(int value) {
        TYPE result = null;

        for (TYPE type : TYPE.values()) {
            if (type.getValue() == value) {
                result = type;
                break;
            }
        }

        return result;
    }

    public enum REQUEST {
        TOPIC(0),
        PARTITION(1);

        private final int value;
        REQUEST(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    public static REQUEST findRequestByValue(int value) {
        REQUEST result = null;

        for (REQUEST type : REQUEST.values()) {
            if (type.getValue() == value) {
                result = type;
                break;
            }
        }

        return result;
    }

    public enum PROPERTY_KEY {
        BROKER(0),
        LOADBALANCER(1),
        OFFSET(2),
        METHOD(3),
        HOST_NAME(4); //optional

        private final int value;
        PROPERTY_KEY(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    public enum METHOD {
        PULL(0),
        PUSH(1);

        private final int value;
        METHOD(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    public static METHOD findMethodByName(String name) {
        METHOD result = null;
        for (METHOD method : METHOD.values()) {
            if (method.name().equalsIgnoreCase(name)) {
                result = method;
                break;
            }
        }
        return result;
    }

    public static class REQUEST_TYPE {
        public static String ADD = "ADD";
        public static String REM = "REMOVE";
        public static String FAIL = "FAIL";
        public static String LEADER = "NEW LEADER";
        public static String ELECTION = "ELECTION";
    }

    public enum BROKER_DESIGNATION {
        LEADER(0),
        FOLLOWER(1);

        private final int value;
        BROKER_DESIGNATION(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    public static class BROKER_STATUS {
        public static String ACTIVE = "ACTIVE";
        public static String INACTIVE = "INACTIVE";
    }

    public static class RESPONSE_STATUS {
        public static String OK = "OK";
        public static String ERROR = "ERROR";
        public static String SYN = "SYN";
        public static List<String> STATUS_LIST = List.of(OK, ERROR, SYN);
    }
}
