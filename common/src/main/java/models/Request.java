package models;

import configuration.Constants;
import utilities.Strings;

public class Request extends Object {
    private int type; //Topic or Partition
    private String topicName;
    private int partition;
    private int offset;

    public Request(int type, String topicName, int partition) {
        this.type = type;
        this.topicName = topicName;
        this.partition = partition;
    }

    public Request(int type, String topicName, int partition, int offset) {
        this.type = type;
        this.topicName = topicName;
        this.partition = partition;
        this.offset = offset;
    }

    public Request() {}

    public int getType() {
        return type;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getPartition() {
        return partition;
    }

    public int getOffset() {
        return offset;
    }

    public boolean isValid() {
        return Constants.findRequestByValue(type) != null && !Strings.isNullOrEmpty(topicName);
    }
}
