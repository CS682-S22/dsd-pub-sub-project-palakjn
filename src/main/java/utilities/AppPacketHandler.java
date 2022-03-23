package utilities;

import configurations.AppConstants;
import models.Topic;

public class AppPacketHandler extends PacketHandler {
    public static byte[] createAddTopicPacket(Topic topic, int seqNum) {
        return createPacket(AppConstants.REQUESTER.TOPIC, AppConstants.TYPE.ADD, topic, seqNum);
    }
}
