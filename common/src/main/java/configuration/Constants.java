package configuration;

public class Constants {
    public static final int START_VALID_PORT = 1700;
    public static final int END_VALID_PORT = 1724;
    public static int RTT = 2000; //Response Turnout time. The time (in milliseconds) host will be waiting for response.

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
        RESP(1), //Response
        ACK(2),  //Acknowledgment
        NACK(3), //Negative Acknowledgement
        DATA(4),
        PULL(5),
        ADD(6),  //Join or Subscribe
        REM(7);  //Remove

        private final int value;
        TYPE(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }
}
