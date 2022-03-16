package utilities;

import configuration.Constants;
import models.Request;

import java.nio.ByteBuffer;

public class ProducerPacketHandler extends PacketHandler {

    public static byte[] createRequest(String topic, int partition) {
        byte[] packet = null;
        Request request = new Request(Constants.REQUEST.PARTITION.getValue(), topic, partition);
        byte[] body = request.toByte();

        if (body != null) {
            byte[] header = createHeader(Constants.REQUESTER.PRODUCER, Constants.TYPE.REQ);
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
