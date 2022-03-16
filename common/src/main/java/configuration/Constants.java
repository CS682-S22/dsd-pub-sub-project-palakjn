package configuration;

public class Constants {
    public static final int START_VALID_PORT = 1700;
    public static final int END_VALID_PORT = 1724;
    public static int RTT = 2000; //Response Turnout time. The time (in milliseconds) host will be waiting for response.
    public static int NUM_OF_THREADS = 10;

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
        ADD(7),  //Join or Subscribe
        REM(8),  //Remove
        FIN(9);

        private final int value;
        TYPE(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
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
}
