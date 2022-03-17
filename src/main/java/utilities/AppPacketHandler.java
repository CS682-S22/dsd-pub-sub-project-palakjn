package utilities;

import configurations.AppConstants;
import models.Request;
import models.Topic;

import java.nio.ByteBuffer;

public class AppPacketHandler extends PacketHandler {
    public static byte[] createAddTopicPacket(Topic topic, int seqNum) {
        return createPacket(AppConstants.REQUESTER.TOPIC, AppConstants.TYPE.ADD, topic, seqNum);
    }

    public static byte[] createGetBrokerReq(AppConstants.REQUESTER requester, String topic, int partition) {
        return createPacket(requester, AppConstants.TYPE.REQ, topic, partition);
    }

    public static byte[] createToBrokerRequest(AppConstants.REQUESTER requester, AppConstants.TYPE type, String topic, int partition) {
        return createPacket(requester, type, topic, partition);
    }

    public static byte[] createToBrokerRequest(AppConstants.REQUESTER requester, AppConstants.TYPE type, String topic, int partition, int offset) {
        return createPacket(requester, type, topic, partition, offset);
    }

    private static byte[] createPacket(AppConstants.REQUESTER requester, AppConstants.TYPE type, String topic, int partition) {
        return createPacket(requester, type, topic, partition, 0);
    }

    private static byte[] createPacket(AppConstants.REQUESTER requester, AppConstants.TYPE type, String topic, int partition, int offset) {
        byte[] packet = null;
        Request request = new Request(AppConstants.REQUEST.PARTITION.getValue(), topic, partition, offset);

        return createPacket(requester, type, request);
    }

    public static byte[] createDataPacket(byte[] data) {
        byte[] header = createHeader(AppConstants.REQUESTER.PRODUCER, AppConstants.TYPE.DATA);
        return ByteBuffer.allocate(4 + header.length + data.length).putInt(header.length).put(header).put(data).array();
    }
}
