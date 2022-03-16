package models;

import configuration.Constants;

import java.util.HashMap;
import java.util.Map;

public class Properties {
    private Map<Constants.PROPERTY_KEY, String> properties;

    public Properties() {
        properties = new HashMap<>();
    }

    public void put(Constants.PROPERTY_KEY key, String value) {
        if (!properties.containsKey(key)) {
            properties.put(key, value);
        }
    }

    public String getValue(Constants.PROPERTY_KEY key) {
        return properties.getOrDefault(key, null);
    }
}
