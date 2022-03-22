package controllers;

/**
 * Subscriber which listens for new events.
 *
 * @author Palak Jain
 */
public interface ISubscriber {

    /**
     * Send message to the consumer
     * @param data
     */
    public void onEvent(byte[] data);
}
