package utilities;

import com.google.protobuf.InvalidProtocolBufferException;
import configuration.Constants;
import models.Header;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Responsible for generating packets to send via the channel from one host to another.
 *
 * @author Palak Jain
 */
public class PacketHandler {

    /**
     * Create the header part of the packet
     * @param requester the type of the node
     * @param type the type of the packet
     * @param seqNum sequence number
     * @return byte array
     */
    public static byte[] createHeader(Constants.REQUESTER requester, Constants.TYPE type, int seqNum) {
        return Header.Content.newBuilder().setRequester(requester.getValue()).setType(type.getValue()).setSeqNum(seqNum).build().toByteArray();
    }

    /**
     * Create the header part of the packet
     * @param requester the type of the node
     * @param type the type of the packet
     * @return byte array
     */
    public static byte[] createHeader(Constants.REQUESTER requester, Constants.TYPE type) {
        return Header.Content.newBuilder().setRequester(requester.getValue()).setType(type.getValue()).build().toByteArray();
    }

    /**
     * Get the header part from the message
     * @param message the received message from the host
     * @return Decoded Header
     */
    public static Header.Content getHeader(byte[] message) {
        Header.Content header = null;

        try {
            ByteBuffer byteBuffer = ByteBuffer.wrap(message);
            //Reading length of the header
            int length = byteBuffer.getInt();
            //Reading main header part
            byte[] headerBytes = new byte[length];
            byteBuffer.get(headerBytes, 0, length);

            header =  Header.Content.parseFrom(headerBytes);
        } catch (InvalidProtocolBufferException | BufferUnderflowException exception) {
            System.err.printf("Unable to read the header part from the received message. Error: %s.\n", exception.getMessage());
        }

        return header;
    }

    /**
     * Get the actual file chunk data from the message
     * @param message the received message from the host
     * @return file data
     */
    public static byte[] getData(byte[] message) {
        byte[] content = null;

        try {
            content = Arrays.copyOfRange(message, getOffset(message), message.length);
        } catch (IndexOutOfBoundsException exception ) {
            System.err.printf("Unable to read the data from the received message. Error: %s.\n", exception.getMessage());
        }

        return content;
    }

    /**
     * Get the offset from where to read the body part of the message.
     * @param message the received message from the host
     * @return offset from where to read the message
     */
    public static int getOffset(byte[] message) {
        int position = 0;

        try {
            //4 + header length will give the position of the array where the next part of the message resides.
            position = 4 + ByteBuffer.wrap(message).getInt();
        } catch (BufferUnderflowException exception) {
            System.err.printf("Unable to get the position to read next bytes after header from the received message. Error: %s.\n", exception.getMessage());
        }

        return position;
    }
}
