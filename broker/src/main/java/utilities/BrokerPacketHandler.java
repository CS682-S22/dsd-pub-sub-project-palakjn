package utilities;

import configuration.Constants;
import configurations.BrokerConstants;
import models.Host;
import models.election.ElectionRequest;
import models.heartbeat.HeartBeatRequest;
import models.requests.BrokerUpdateRequest;
import models.requests.Request;
import models.requests.TopicReadWriteRequest;
import models.responses.Response;
import models.sync.DataPacket;
import models.sync.OffsetRequest;
import models.sync.OffsetResponse;

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
    public static byte[] createElectionPacket(String key, Host failedBroker) {
        ElectionRequest electionRequest = new ElectionRequest(key, failedBroker);
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

    /**
     * Create the packet to request for the offset from the broker
     */
    public static byte[] createOffsetRequest(String key) {
        OffsetRequest offsetRequest = new OffsetRequest(key);
        Request<OffsetRequest> request = new Request<>(BrokerConstants.REQUEST_TYPE.OFFSET, offsetRequest);

        return createPacket(BrokerConstants.REQUESTER.BROKER, BrokerConstants.TYPE.REQ, request);
    }

    /**
     * Create the response of offset request to send it to the broker
     */
    public static byte[] createOffsetResponse(String key, int offset, int size) {
        OffsetResponse offsetResponse = new OffsetResponse(key, offset, size);
        Response<OffsetResponse> response = new Response<>(BrokerConstants.RESPONSE_STATUS.OK, offsetResponse);

        return createPacket(BrokerConstants.REQUESTER.BROKER, BrokerConstants.TYPE.RESP, response);
    }

    /**
     * Create the request to get the data from the broker from the given fromOffset
     */
    public static byte[] createGetDataRequest(String key, int fromOffset, int toOffset) {
        String parts[] = key.split(":");
        TopicReadWriteRequest topicReadWriteRequest = new TopicReadWriteRequest(parts[0], Integer.parseInt(parts[1]), fromOffset);
        topicReadWriteRequest.setToOffset(toOffset);
        Request<TopicReadWriteRequest> request = new Request<>(BrokerConstants.REQUEST_TYPE.GET, topicReadWriteRequest);

        return createPacket(BrokerConstants.REQUESTER.BROKER, BrokerConstants.TYPE.PULL, request);
    }

    /**
     * Create the packet to contain the data packet to send to broker
     */
    public static byte[] createDataPacket(String key, String type, byte[] data, int toOffset) {
        DataPacket dataPacket = new DataPacket(key, type, data, toOffset);

        return createPacket(BrokerConstants.REQUESTER.BROKER, BrokerConstants.TYPE.DATA, dataPacket);
    }

    /**
     * Create the packet to contain the data packet to send to broker
     */
    public static byte[] createDataPacket(String key, String type, byte[] data) {
        DataPacket dataPacket = new DataPacket(key, type, data);

        return createPacket(BrokerConstants.REQUESTER.BROKER, BrokerConstants.TYPE.DATA, dataPacket);
    }
}
