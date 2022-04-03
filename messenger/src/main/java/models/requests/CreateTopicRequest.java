package models.requests;

import utilities.Strings;

/**
 * Responsible for holding topic information to create.
 *
 * @author Palak Jain
 */
public class CreateTopicRequest {
    private String name;
    private int numOfPartitions;
    private int numOfFollowers;

    public CreateTopicRequest(String name, int numOfPartitions, int numOfFollowers) {
        this.name = name;
        this.numOfPartitions = numOfPartitions;
        this.numOfFollowers = numOfFollowers;
    }

    /**
     * Get the name of the topic
     */
    public String getName() {
        return name;
    }

    /**
     * Get number of partitions to create
     */
    public int getNumOfPartitions() {
        return numOfPartitions;
    }

    /**
     * Get number of replicas to create for each partitions
     */
    public int getNumOfFollowers() {
        return numOfFollowers;
    }

    /**
     * Checks if the request is valid or not
     */
    public boolean isValid() {
        return !Strings.isNullOrEmpty(name) && numOfPartitions > 0;
    }
}
