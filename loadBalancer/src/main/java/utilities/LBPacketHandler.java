package utilities;

import configuration.Constants;
import models.Object;

/**
 * Responsible for creating packets to send to/for parsing packets which are received.
 *
 * @author Palak Jain
 */
public class LBPacketHandler extends PacketHandler {

    /**
     * Create packet to send to another host
     */
    public static byte[] createPacket(Constants.TYPE type, Object object) {
        return createPacket(Constants.REQUESTER.LOAD_BALANCER, type, object);
    }
}
