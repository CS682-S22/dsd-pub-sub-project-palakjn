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
}
