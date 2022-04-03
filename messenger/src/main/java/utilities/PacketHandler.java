package utilities;

import com.google.protobuf.InvalidProtocolBufferException;
import configuration.Constants;
import models.Header;
import models.Object;
import models.requests.GetBrokerRequest;
import models.requests.TopicReadWriteRequest;
import models.requests.Request;

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
     * Create the header part of the packet
     * @param requester the type of the node
     * @param type the type of the packet
     * @return byte array
     */
    public static byte[] createHeaderWithOffset(Constants.REQUESTER requester, Constants.TYPE type, int offset) {
        return Header.Content.newBuilder().setRequester(requester.getValue()).setType(type.getValue()).setOffset(offset).build().toByteArray();
    }

    /**
     * Creates an acknowledgement packet for the file chunk with the given sequence number
     * @param requester the type of the requester: BROKER, LOADBALANCER, etc
     * @param seqNum Sequence number of the file whose acknowledgement we are sending
     * @return the packet
     */
    public static byte[] createACK(Constants.REQUESTER requester, int seqNum) {
        byte[] header = createHeader(requester, Constants.TYPE.ACK, seqNum);

        return ByteBuffer.allocate(4 + header.length).putInt(header.length).put(header).array();
    }

    /**
     * Creates negative acknowledgement packet for the file chunk with the given sequence number
     * @param requester the type of the requester: BROKER, LOADBALANCER, etc
     * @param seqNum Sequence number of the file whose acknowledgement we are sending
     * @return the packet
     */
    public static byte[] createNACK(Constants.REQUESTER requester, int seqNum) {
        byte[] header = createHeader(requester, Constants.TYPE.NACK, seqNum);

        return ByteBuffer.allocate(4 + header.length).putInt(header.length).put(header).array();
    }

    /**
     * Creates negative acknowledgement packet for the file chunk
     * @param requester the type of the requester: BROKER, LOADBALANCER, etc
     * @return the packet
     */
    public static byte[] createNACK(Constants.REQUESTER requester) {
        byte[] header = createHeader(requester, Constants.TYPE.NACK);

        return ByteBuffer.allocate(4 + header.length).putInt(header.length).put(header).array();
    }

    /**
     * Create the entire packet: header + body
     * @return byte array
     */
    public static byte[] createPacket(Constants.REQUESTER requester, Constants.TYPE type, Object object) {
        byte[] packet = null;
        byte[] body = object.toByte();

        if (body != null) {
            byte[] header = createHeader(requester, type);
            packet = ByteBuffer.allocate(4 + header.length + body.length).putInt(header.length).put(header).put(body).array();
        }

        return packet;
    }

    /**
     * Create the entire packet with the seqNum: header + body
     * @return byte array
     */
    public static byte[] createPacket(Constants.REQUESTER requester, Constants.TYPE type, Object object, int seqNum) {
        byte[] packet = null;
        byte[] body = object.toByte();

        if (body != null) {
            byte[] header = createHeader(requester, type, seqNum);
            packet = ByteBuffer.allocate(4 + header.length + body.length).putInt(header.length).put(header).put(body).array();
        }

        return packet;
    }

    /**
     * Create the data packet
     */
    public static byte[] createDataPacket(Constants.REQUESTER requester, byte[] data) {
        byte[] header = createHeader(requester, Constants.TYPE.DATA);
        return ByteBuffer.allocate(4 + header.length + data.length).putInt(header.length).put(header).put(data).array();
    }

    /**
     * Create request to get broker information which is holding topic-partition information
     */
    public static byte[] createGetBrokerReq(Constants.REQUESTER requester, String topic, int partition) {
        GetBrokerRequest request = new GetBrokerRequest(topic, partition);

        return createPacket(requester, Constants.TYPE.REQ, new Request<>(request));
    }

    /**
     * Create request to the broker which is holding the topic-partition information
     */
    public static byte[] createToBrokerRequest(Constants.REQUESTER requester, Constants.TYPE type, String topic, int partition) {
        return createPacket(requester, type, topic, partition);
    }

    /**
     * Create request to the broker which is holding the topic-partition information. Mention the offset to read from in the data
     */
    public static byte[] createToBrokerRequest(Constants.REQUESTER requester, Constants.TYPE type, String topic, int partition, int offset) {
        return createPacket(requester, type, topic, partition, offset, 0);
    }

    /**
     * Create request to the broker which is holding the topic-partition information. Mention the offset to read from in the data plus number of records to read
     */
    public static byte[] createToBrokerRequest(Constants.REQUESTER requester, Constants.TYPE type, String topic, int partition, int offset, int numOfRecords) {
        return createPacket(requester, type, topic, partition, offset, numOfRecords);
    }

    /**
     * Create request to the broker which is holding the topic-partition information.
     */
    private static byte[] createPacket(Constants.REQUESTER requester, Constants.TYPE type, String topic, int partition) {
        return createPacket(requester, type, topic, partition, 0, 0);
    }

    /**
     * Create request to the broker which is holding the topic-partition information. Mention the offset to read from in the data plus number of records to read
     */
    private static byte[] createPacket(Constants.REQUESTER requester, Constants.TYPE type, String topic, int partition, int offset, int numOfRecords) {
        TopicReadWriteRequest readTopicRequest = new TopicReadWriteRequest(topic, partition, offset, numOfRecords);

        return createPacket(requester, type, new Request<>(readTopicRequest));
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
