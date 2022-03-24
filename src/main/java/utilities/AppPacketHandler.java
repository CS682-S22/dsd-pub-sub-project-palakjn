package utilities;

import configurations.AppConstants;
import models.Topic;

/**
 * Responsible for creating new packets to send/parsing received packets.
 *
 * @author Palak Jain
 */
public class AppPacketHandler extends PacketHandler {

    /**
     * Create packet to add topic
     */
    public static byte[] createAddTopicPacket(Topic topic, int seqNum) {
        return createPacket(AppConstants.REQUESTER.TOPIC, AppConstants.TYPE.ADD, topic, seqNum);
    }
}
