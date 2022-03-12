package utilities;

import configuration.Constants;
import models.Host;

import java.nio.ByteBuffer;

public class BrokerPacketHandler extends PacketHandler {

    public static byte[] createJoinPacket(Host brokerInfo) {
        byte[] packet = null;
        byte[] brokerInBytes = brokerInfo.toByte();

        if (brokerInBytes != null) {
            byte[] header = createHeader(Constants.REQUESTER.BROKER, Constants.TYPE.ADD);
            packet = ByteBuffer.allocate(4 + header.length + brokerInBytes.length).putInt(header.length).put(header).put(brokerInBytes).array();
        }

        return packet;
    }
}
