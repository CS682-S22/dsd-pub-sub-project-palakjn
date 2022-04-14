package controllers.connections;

import controllers.Connection;

import java.util.HashMap;
import java.util.Map;

/**
 * Responsible for adding new channel and reusing existing channels
 *
 * @author Palak Jain
 */
public class Channels {
    private static Map<String, Connection> channels = new HashMap<>();

    /**
     * Add new channel
     */
    public static void add(String key, Connection connection) {
        channels.put(key, connection);
    }

    /**
     * Get the existing connection if exist
     */
    public static Connection get(String key) {
        return channels.getOrDefault(key, null);
    }
}
