package models;

import configuration.Constants;
import utilities.Strings;

public class Request extends Object {
    private int type; //Topic or Partition
    private String topicName;
    private int partition;

    public Request(int type, String topicName, int partition) {
        this.type = type;
        this.topicName = topicName;
        this.partition = partition;
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

    public boolean isValid() {
        return Constants.findRequestByValue(type) != null && !Strings.isNullOrEmpty(topicName);
    }
}
