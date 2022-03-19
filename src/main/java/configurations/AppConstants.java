package configurations;

import configuration.Constants;

public class AppConstants extends Constants {
    public static int THREAD_COUNT = 1;
    public static int QUEUE_BUFFER_SIZE = 1000;
    public static int WAIT_TIME = 1000;
    public static int CONSUMER_WAIT_TIME = 5000;

    public enum METHOD {
        PULL(0),
        POST(1);

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
}
