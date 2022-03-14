package utilities;

import configuration.Constants;
import models.Object;

public class LBPacketHandler extends PacketHandler {

    public static byte[] createPacket(Constants.TYPE type, Object object) {
        return createPacket(Constants.REQUESTER.LOAD_BALANCER, type, object);
    }
}
