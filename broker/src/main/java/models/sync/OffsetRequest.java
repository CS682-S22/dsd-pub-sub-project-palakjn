package models.sync;

import com.google.gson.annotations.Expose;
import utilities.Strings;

/**
 * Responsible for holding the values need to ask for the offset for the partition of the topic
 *
 * @author Palak Jain
 */
public class OffsetRequest {
    @Expose
    private String key;

    public OffsetRequest(String key) {
        this.key = key;
    }

    /**
     * Get the partition key of the topic
     */
    public String getKey() {
        return key;
    }

    /**
     * Checks whether the request is valid or invalid
     */
    public boolean isValid() {
        return !Strings.isNullOrEmpty(key);
    }
}
