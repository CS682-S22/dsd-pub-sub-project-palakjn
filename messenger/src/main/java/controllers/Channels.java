package controllers;

import configuration.Constants;

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
    private static Map<String, Connection> brokerChannels = new HashMap<>();
    private static Connection loadBalancer;

    /**
     * Add new channel
     */
    public synchronized static void add(String key, Connection connection, Constants.CHANNEL_TYPE channel_type) {
        if (channel_type == Constants.CHANNEL_TYPE.HEARTBEAT && !heartBeatChannels.containsKey(key)) {
            heartBeatChannels.put(key, connection);
        } else if (channel_type == Constants.CHANNEL_TYPE.DATA && !dataChannels.containsKey(key)) {
            dataChannels.put(key, connection);
        } else if (channel_type == Constants.CHANNEL_TYPE.BROKER && !dataChannels.containsKey(key)) {
            brokerChannels.put(key, connection);
        } else if (channel_type == Constants.CHANNEL_TYPE.LOADBALANCER) {
            loadBalancer = connection;
        }
    }

    /**
     * Get the existing connection if exist
     */
    public synchronized static Connection get(String key, Constants.CHANNEL_TYPE channel_type) {
        Connection connection = null;

        if (channel_type == Constants.CHANNEL_TYPE.HEARTBEAT) {
            connection = heartBeatChannels.getOrDefault(key, null);
        } else if (channel_type == Constants.CHANNEL_TYPE.DATA) {
            connection = dataChannels.getOrDefault(key, null);
        } else if (channel_type == Constants.CHANNEL_TYPE.BROKER) {
            connection = brokerChannels.getOrDefault(key, null);
        } else if (channel_type == Constants.CHANNEL_TYPE.LOADBALANCER) {
            connection = loadBalancer;
        }

        return connection;
    }

    /**
     * Add new channel if exist or update existing one
     */
    public synchronized static void upsert(String key, Connection connection, Constants.CHANNEL_TYPE channel_type) {
        if (channel_type == Constants.CHANNEL_TYPE.HEARTBEAT) {
            heartBeatChannels.put(key, connection);
        } else if (channel_type == Constants.CHANNEL_TYPE.DATA) {
            dataChannels.put(key, connection);
        } else if (channel_type == Constants.CHANNEL_TYPE.BROKER) {
            brokerChannels.put(key, connection);
        } else if (channel_type == Constants.CHANNEL_TYPE.LOADBALANCER) {
            loadBalancer = connection;
        }
    }

    /**
     * Remove the channel information from the collection
     */
    public synchronized static void remove(String key, Constants.CHANNEL_TYPE channel_type) {
        if (channel_type == Constants.CHANNEL_TYPE.HEARTBEAT) {
            heartBeatChannels.remove(key);
        } else if (channel_type == Constants.CHANNEL_TYPE.DATA) {
            dataChannels.remove(key);
        } else if (channel_type == Constants.CHANNEL_TYPE.BROKER) {
            brokerChannels.remove(key);
        } else if (channel_type == Constants.CHANNEL_TYPE.LOADBALANCER) {
            loadBalancer = null;
        }
    }

    /**
     * Close and remove all the DATA\HEARTBEAT connections with the given serverId
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
