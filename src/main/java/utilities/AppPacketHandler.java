package utilities;

import configuration.Constants;
import models.Request;
import models.Topic;

import java.nio.ByteBuffer;

public class AppPacketHandler extends PacketHandler {
    public static byte[] createAddTopicPacket(Topic topic, int seqNum) {
        return createPacket(Constants.REQUESTER.TOPIC, Constants.TYPE.ADD, topic, seqNum);
    }

    public static byte[] createGetBrokerReq(String topic, int partition) {
        return createPacket(Constants.TYPE.REQ, topic, partition);
    }

    public static byte[] createToBrokerRequest(String topic, int partition) {
        return createPacket(Constants.TYPE.ADD, topic, partition);
    }

    private static byte[] createPacket(Constants.TYPE type, String topic, int partition) {
        byte[] packet = null;
        Request request = new Request(Constants.REQUEST.PARTITION.getValue(), topic, partition);
        byte[] body = request.toByte();

        if (body != null) {
            byte[] header = createHeader(Constants.REQUESTER.PRODUCER, type);
            packet = ByteBuffer.allocate(4 + header.length + body.length).putInt(header.length).put(header).put(body).array();
        }

        return packet;
    }

    public static byte[] createDataPacket(byte[] data) {
        byte[] header = createHeader(Constants.REQUESTER.PRODUCER, Constants.TYPE.DATA);
        return ByteBuffer.allocate(4 + header.length + data.length).putInt(header.length).put(header).put(data).array();
    }

    public static byte[] createFINPacket() {
        byte[] header = createHeader(Constants.REQUESTER.PRODUCER, Constants.TYPE.FIN);
        return ByteBuffer.allocate(4 + header.length).putInt(header.length).put(header).array();
    }
}
