package models.sync;

import com.google.gson.annotations.Expose;
import models.Object;
import utilities.Strings;

/**
 * Responsible for holding the data to pass between brokers as part of sync process
 *
 * @author Palak Jain
 */
public class DataPacket extends Object {
    @Expose
    private String key;
    @Expose
    private String dataType;
    @Expose
    private byte[] data;
    @Expose
    private int toOffset;

    public DataPacket(String key, String dataType, byte[] data, int toOffset) {
        this.key = key;
        this.dataType = dataType;
        this.data = data;
        this.toOffset = toOffset;
    }

    /**
     * Get the partition key
     */
    public String getKey() {
        return key;
    }

    /**
     * Get the data type (Either: Catch up or replica)
     */
    public String getDataType() {
        return dataType;
    }

    /**
     * Get the data in bytes
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Get the offset of last expecting data
     */
    public int getToOffset() {
        return toOffset;
    }

    /**
     * Determines whether the packet is valid
     */
    public boolean isValid() {
        return !Strings.isNullOrEmpty(key) && !Strings.isNullOrEmpty(dataType) && data != null && toOffset > 0;
    }
}
