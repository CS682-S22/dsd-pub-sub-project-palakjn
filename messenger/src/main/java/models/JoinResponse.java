package models;

/**
 * Responsible for holding the values being passed to the broker as part of the JOIN request
 */
public class JoinResponse extends Object {
    private int priorityNum;

    public JoinResponse(int priorityNum) {
        this.priorityNum = priorityNum;
    }

    /**
     * Get the priority number of the host
     */
    public int getPriorityNum() {
        return priorityNum;
    }
}
