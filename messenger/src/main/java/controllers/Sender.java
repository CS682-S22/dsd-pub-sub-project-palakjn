package controllers;

/**
 * Interface for sending messages
 *
 * @author Palak Jain
 */
public interface Sender {
    public boolean send(byte[] message);
}
