package controllers;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

/**
 * Responsible for sending and receiving data from the channel being established between two hosts.
 *
 * @author Palak Jain
 */
public class Connection implements Sender, Receiver {

    private Socket channel;
    protected String destinationIPAddress;
    protected int destinationPort;
    protected String sourceIPAddress;
    protected int sourcePort;
    private DataInputStream inputStream;
    private DataOutputStream outputStream;
    private boolean isClosed;

    public Connection(Socket channel, String destinationIPAddress, int destinationPort, String sourceIPAddress, int sourcePort) {
        this.channel = channel;
        this.destinationIPAddress = destinationIPAddress;
        this.destinationPort = destinationPort;
        this.sourceIPAddress = sourceIPAddress;
        this.sourcePort = sourcePort;
    }

    public Connection(Socket channel, String destinationIPAddress, int destinationPort) {
        this.channel = channel;
        this.destinationIPAddress = destinationIPAddress;
        this.destinationPort = destinationPort;
    }

    public Connection() {}

    /**
     * Opens the connection with another host by opening the input stream or output stream based on the requirement
     * @return true if successful else false
     */
    public boolean openConnection() {
        boolean isSuccess = false;

        try {
            inputStream = new DataInputStream(channel.getInputStream());
            outputStream = new DataOutputStream(channel.getOutputStream());
            isSuccess = true;
        } catch (IOException exception) {
            System.err.printf("[%s:%d] Unable to get input/output stream. Error: %s.\n", sourceIPAddress, sourcePort, exception.getMessage());
        }

        return isSuccess;
    }

    /**
     * @return the destination host address
     */
    public String getDestinationIPAddress() {
        return destinationIPAddress;
    }

    /**
     * @return the destination port number
     */
    public int getDestinationPort() {
        return destinationPort;
    }

    /**
     * @return the source host address
     */
    public String getSourceIPAddress() {
        return sourceIPAddress;
    }

    /**
     * @return the source port
     */
    public int getSourcePort() {
        return sourcePort;
    }

    /**
     * @return true is socket is still open else false
     */
    public boolean isOpen() {
        return !isClosed;
    }

    /**
     * Sending message to another host
     * @param message
     * @return true if successful else false
     */
    @Override
    public boolean send(byte[] message) {
        boolean isSend = false;

        try {
            outputStream.writeInt(message.length);
            outputStream.write(message);
            isSend = true;
        } catch (SocketException exception) {
            //If getting socket exception means connection is refused or cancelled. In this case, will not attempt to make any operation
            isClosed = true;
        } catch (IOException exception) {
            System.err.printf("[%s:%d] Fail to send message to %s:%d. Error: %s.\n", sourceIPAddress, sourcePort, destinationIPAddress, destinationPort, exception.getMessage());
        }

        return isSend;
    }

    /**
     * Receive message from another host.
     * @return the message being received
     */
    @Override
    public byte[] receive() {
        byte[] buffer = null;

        try {
            int length = inputStream.readInt();
            if(length > 0) {
                buffer = new byte[length];
                inputStream.readFully(buffer, 0, buffer.length);
            }
        } catch (EOFException | SocketTimeoutException ignored) {} //No more content available to read
        catch (SocketException exception) {
            //If getting socket exception means connection is refused or cancelled. In this case, will not attempt to make any operation
            isClosed = true;
        } catch (IOException exception) {
            System.err.printf("[%s:%d] Fail to receive message from %s:%d. Error: %s.\n", sourceIPAddress, sourcePort, destinationIPAddress, destinationPort, exception.getMessage());
        }

        return buffer;
    }

    /**
     * Finds out if there are some bytes to read from the incoming channel
     */
    public boolean isAvailable() {
        boolean isAvailable = false;

        try {
            isAvailable = inputStream.available() != 0;
        } catch (IOException e) {
            System.err.printf("Unable to get the available bytes to read. Error: %s", e.getMessage());
        }

        return isAvailable;
    }

    /**
     * CLose the connection between two hosts
     */
    public void closeConnection() {
        try {
            if(inputStream != null) inputStream.close();
            if(outputStream != null) outputStream.close();
            channel.close();
        } catch (IOException e) {
            System.err.printf("[%s:%d] Unable to close the connection. Error: %s", sourceIPAddress, sourcePort, e.getMessage());
        }
    }
}
