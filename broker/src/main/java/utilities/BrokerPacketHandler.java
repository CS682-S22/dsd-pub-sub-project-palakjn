package utilities;

import configurations.BrokerConstants;
import models.Host;

import java.nio.ByteBuffer;

/**
 * Responsible for creating new packets to send/parsing received packets.
 *
 * @author Palak Jain
 */
public class BrokerPacketHandler extends PacketHandler {

    /**
     * Creates packet ADD/REM to/from the network to send to load balancer
     */
    public static byte[] createPacket(Host brokerInfo, BrokerConstants.TYPE type) {

        return createPacket(BrokerConstants.REQUESTER.BROKER, type, brokerInfo);
    }

    /**
     * Creates the data packet containing next offset to read to
     */
    public static byte[] createDataPacket(byte[] data, int offset) {
        byte[] packet = null;

        if (data != null) {
            byte[] header = createHeaderWithOffset(BrokerConstants.REQUESTER.BROKER, BrokerConstants.TYPE.DATA, offset);
            packet = ByteBuffer.allocate(4 + header.length + data.length).putInt(header.length).put(header).put(data).array();
        }

        return packet;
    }

    /**
     * Creates the data packet to send to
     */
    public static byte[] createDataPacket(byte[] data) {
         return createDataPacket(BrokerConstants.REQUESTER.BROKER, data);
    }
}
