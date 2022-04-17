package utilities;

import configuration.Constants;
import configurations.BrokerConstants;
import models.ElectionRequest;
import models.HeartBeatRequest;
import models.Host;
import models.requests.BrokerUpdateRequest;
import models.requests.Request;

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
    public static byte[] createPacket(Host brokerInfo, String type) {
        Request<Host> request = new Request<>(type, brokerInfo);

        return createPacket(BrokerConstants.REQUESTER.BROKER, BrokerConstants.TYPE.REQ, request);
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

    /**
     * Create the heartbeat packet to send to broker
     */
    public static byte[] createHeartBeatPacket(HeartBeatRequest request) {
        return createPacket(BrokerConstants.REQUESTER.BROKER, BrokerConstants.TYPE.HEARTBEAT, request);
    }

    /**
     * Create the election packet to send to high priority brokers
     */
    public static byte[] createElectionPacket(String key) {
        ElectionRequest electionRequest = new ElectionRequest(key);
        Request<ElectionRequest> request = new Request<>(BrokerConstants.REQUEST_TYPE.ELECTION, electionRequest);

        return createPacket(BrokerConstants.REQUESTER.BROKER, BrokerConstants.TYPE.ELECTION, request);
    }

    /**
     * Create the packet to indicate other hosts about the new leader
     */
    public static byte[] createLeaderUpdateRequest(String key, Host newLeader) {
        BrokerUpdateRequest brokerUpdateRequest = new BrokerUpdateRequest(key, newLeader);
        Request<BrokerUpdateRequest> request = new Request<>(BrokerConstants.REQUEST_TYPE.LEADER, brokerUpdateRequest);

        return createPacket(BrokerConstants.REQUESTER.BROKER, BrokerConstants.TYPE.UPDATE, request);
    }

    /**
     * Create the packet to indicate load balancer the failure of the broker
     */
    public static byte[] createFailBrokerPacket(String key, Host broker) {
        BrokerUpdateRequest brokerUpdateRequest = new BrokerUpdateRequest(key, broker);
        Request<BrokerUpdateRequest> request = new Request<>(BrokerConstants.REQUEST_TYPE.FAIL, brokerUpdateRequest);

        return createPacket(BrokerConstants.REQUESTER.BROKER, BrokerConstants.TYPE.UPDATE, request);
    }
}
