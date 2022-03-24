package models;

import configuration.Constants;

import java.util.HashMap;
import java.util.Map;

/**
 * Responsible for holding properties which are needed by producer/Consumer to run
 *
 * @author Palak Jain
 */
public class Properties {
    private Map<Constants.PROPERTY_KEY, String> properties;

    public Properties() {
        properties = new HashMap<>();
    }

    /**
     * Add new key-value pair property
     */
    public void put(Constants.PROPERTY_KEY key, String value) {
        if (!properties.containsKey(key)) {
            properties.put(key, value);
        }
    }

    /**
     * Get the property value for the specified key
     */
    public String getValue(Constants.PROPERTY_KEY key) {
        return properties.getOrDefault(key, null);
    }
}
