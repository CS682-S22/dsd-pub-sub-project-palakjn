package utilities;

import configurations.AppConstants;
import models.requests.CreateTopicRequest;
import models.requests.Request;

/**
 * Responsible for creating new packets to send/parsing received packets.
 *
 * @author Palak Jain
 */
public class AppPacketHandler extends PacketHandler {

    /**
     * Create packet to add topic
     */
    public static byte[] createAddTopicPacket(CreateTopicRequest topicRequest, int seqNum) {
        Request<CreateTopicRequest> request = new Request<>(AppConstants.REQUEST_TYPE.ADD, topicRequest);

        return createPacket(AppConstants.REQUESTER.TOPIC, AppConstants.TYPE.REQ, request, seqNum);
    }
}
