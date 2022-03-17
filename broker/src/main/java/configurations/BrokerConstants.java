package configurations;

import configuration.Constants;

public class BrokerConstants extends Constants {
    public static String TOPIC_LOCATION = "data/%s_%s";
    public static int MAX_SEGMENT_MESSAGES = 10;
    public static long SEGMENT_FLUSH_TIME = 500;
    public static long SLEEP_TIME = 1000;
}
