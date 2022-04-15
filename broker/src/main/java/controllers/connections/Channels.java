package controllers.connections;

import configurations.BrokerConstants;
import controllers.Connection;

import java.util.HashMap;
import java.util.Map;

/**
 * Responsible for adding new channel and reusing existing channels
 *
 * @author Palak Jain
 */
public class Channels {
    private static Map<String, Connection> heartBeatChannels = new HashMap<>();
    private static Map<String, Connection> dataChannels = new HashMap<>();

    /**
     * Add new channel
     */
    public synchronized static void add(String key, Connection connection, BrokerConstants.CHANNEL_TYPE channel_type) {
        if (channel_type == BrokerConstants.CHANNEL_TYPE.HEARTBEAT && !heartBeatChannels.containsKey(key)) {
            heartBeatChannels.put(key, connection);
        } else if (channel_type == BrokerConstants.CHANNEL_TYPE.DATA && !dataChannels.containsKey(key)) {
            dataChannels.put(key, connection);
        }
    }

    /**
     * Get the existing connection if exist
     */
    public synchronized static Connection get(String key, BrokerConstants.CHANNEL_TYPE channel_type) {
        Connection connection = null;

        if (channel_type == BrokerConstants.CHANNEL_TYPE.HEARTBEAT) {
            connection = heartBeatChannels.getOrDefault(key, null);
        } else if (channel_type == BrokerConstants.CHANNEL_TYPE.DATA) {
            connection = dataChannels.getOrDefault(key, null);
        }

        return connection;
    }

    /**
     * Add new channel if exist or update existing one
     */
    public synchronized static void upsert(String key, Connection connection, BrokerConstants.CHANNEL_TYPE channel_type) {
        if (channel_type == BrokerConstants.CHANNEL_TYPE.HEARTBEAT) {
            heartBeatChannels.put(key, connection);
        } else if (channel_type == BrokerConstants.CHANNEL_TYPE.DATA) {
            dataChannels.put(key, connection);
        }
    }

    /**
     * Remove the channel information from the collection
     */
    public synchronized static void remove(String key, BrokerConstants.CHANNEL_TYPE channel_type) {
        if (channel_type == BrokerConstants.CHANNEL_TYPE.HEARTBEAT) {
            heartBeatChannels.remove(key);
        } else if (channel_type == BrokerConstants.CHANNEL_TYPE.DATA) {
            dataChannels.remove(key);
        }
    }

    /**
     * Close and remove all the connections with the given serverId
     */
    public synchronized static void remove(String key) {
        Connection connection = heartBeatChannels.getOrDefault(key, null);

        if (connection != null) {
            connection.closeConnection();
            heartBeatChannels.remove(key);
        }

        connection = dataChannels.getOrDefault(key, null);

        if (connection != null) {
            connection.closeConnection();
            dataChannels.remove(key);
        }
    }
}
