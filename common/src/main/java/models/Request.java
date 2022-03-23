package models;

import configuration.Constants;
import utilities.Strings;

public class Request extends Object {
    private int type; //Topic or Partition
    private String topicName;
    private int partition;
    private int offset;
    private int numOfMsg;

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
    public Request(int type, String topicName, int partition, int offset, int numOfMsg) {
        this.type = type;
        this.topicName = topicName;
        this.partition = partition;
        this.offset = offset;
        this.numOfMsg = numOfMsg;
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

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getNumOfMsg() {
        return numOfMsg;
    }

    public boolean isValid() {
        return Constants.findRequestByValue(type) != null && !Strings.isNullOrEmpty(topicName);
    }
}
