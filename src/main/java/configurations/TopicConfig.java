package configurations;

import utilities.Strings;

/**
 * Responsible for holding topics to produce/consume information.
 *
 * @author Palak Jain
 */
public class TopicConfig {
    public String name;
    public int key;
    public String offset;
    public String location;

    /**
     * Get the name of the topic
     */
    public String getName() {
        return name;
    }

    /**
     * Get the partition number
     */
    public int getKey() {
        return key;
    }

    /**
     * Get the offset from where to read to
     */
    public String getOffset() {
        return offset;
    }

    /**
     * Get the location from where to read/ to write
     */
    public String getLocation() {
        return location;
    }

    /**
     * Checks whether the values are valid
     */
    public boolean isValid() {
        return !Strings.isNullOrEmpty(name) && key >= 0 && !Strings.isNullOrEmpty(location);
    }
}
