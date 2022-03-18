package configurations;

import utilities.Strings;

public class TopicConfig {
    public String name;
    public int key;
    public String offset;
    public String location;

    public String getName() {
        return name;
    }

    public int getKey() {
        return key;
    }

    public String getOffset() {
        return offset;
    }

    public String getLocation() {
        return location;
    }

    public boolean isValid() {
        return !Strings.isNullOrEmpty(name) && key >= 0 && !Strings.isNullOrEmpty(location);
    }
}
