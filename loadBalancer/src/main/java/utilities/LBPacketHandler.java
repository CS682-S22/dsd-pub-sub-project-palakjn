package utilities;

import configuration.Constants;
import models.Object;

import java.nio.ByteBuffer;

public class LBPacketHandler extends PacketHandler {

    public static byte[] createResponse(Object object) {
        byte[] packet = null;
        byte[] body = object.toByte();

        if (body != null) {
            byte[] header = createHeader(Constants.REQUESTER.LOAD_BALANCER, Constants.TYPE.RESP);
            packet = ByteBuffer.allocate(4 + header.length + body.length).putInt(header.length).put(header).put(body).array();
        }

        return packet;
    }
}
