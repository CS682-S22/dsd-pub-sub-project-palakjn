package models;

public class Partition extends Object {
    private String topicName;
    private int number;
    private Host broker;

    public Partition(String topicName, int number, Host broker) {
        this.topicName = topicName;
        this.number = number;
        this.broker = broker;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getNumber() {
        return number;
    }

    public Host getBroker() {
        return broker;
    }

    public String getString() {
        return String.format("%s:%d", topicName, number);
    }
}
