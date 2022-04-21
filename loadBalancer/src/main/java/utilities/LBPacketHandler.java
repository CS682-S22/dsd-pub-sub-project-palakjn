package utilities;

import configuration.Constants;
import models.JoinResponse;
import models.Object;
import models.responses.Response;

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

    /**
     * Create packet to send join response to the broker
     */
    public static byte[] createJoinResponse(int priorityNum) {
        Response<JoinResponse> joinResponse = new Response<>(Constants.RESPONSE_STATUS.OK, new JoinResponse(priorityNum));

        return createPacket(Constants.REQUESTER.LOAD_BALANCER, Constants.TYPE.RESP, joinResponse);
    }
}
