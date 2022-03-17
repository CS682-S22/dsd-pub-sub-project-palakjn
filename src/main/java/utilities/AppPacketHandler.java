package utilities;

import configuration.Constants;
import models.Topic;

public class AppPacketHandler extends PacketHandler {
    public static byte[] createAddTopicPacket(Topic topic, int seqNum) {
        return createPacket(Constants.REQUESTER.TOPIC, Constants.TYPE.ADD, topic, seqNum);
    }
}
