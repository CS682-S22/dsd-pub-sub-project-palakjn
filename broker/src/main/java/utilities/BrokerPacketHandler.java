package utilities;

import configurations.BrokerConstants;
import models.Host;

import java.nio.ByteBuffer;

public class BrokerPacketHandler extends PacketHandler {

    public static byte[] createPacket(Host brokerInfo, BrokerConstants.TYPE type) {
        byte[] packet = null;
        byte[] brokerInBytes = brokerInfo.toByte();

        if (brokerInBytes != null) {
            byte[] header = createHeader(BrokerConstants.REQUESTER.BROKER, type);
            packet = ByteBuffer.allocate(4 + header.length + brokerInBytes.length).putInt(header.length).put(header).put(brokerInBytes).array();
        }

        return packet;
    }

    public static byte[] createDataPacket(byte[] data, int offset) {
        byte[] packet = null;
        byte[] body = data;

        if (body != null) {
            byte[] header = createHeaderWithOffset(BrokerConstants.REQUESTER.BROKER, BrokerConstants.TYPE.DATA, offset);
            packet = ByteBuffer.allocate(4 + header.length + body.length).putInt(header.length).put(header).put(body).array();
        }

        return packet;
    }

    public static byte[] createDataPacket(byte[] data) {
        byte[] packet = null;
        byte[] body = data;

        if (body != null) {
            byte[] header = createHeader(BrokerConstants.REQUESTER.BROKER, BrokerConstants.TYPE.DATA);
            packet = ByteBuffer.allocate(4 + header.length + body.length).putInt(header.length).put(header).put(body).array();
        }

        return packet;
    }
}
