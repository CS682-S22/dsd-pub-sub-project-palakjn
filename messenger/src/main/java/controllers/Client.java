package controllers;

import configuration.Constants;
import models.Header;
import models.Host;
import models.Partition;
import models.Properties;
import org.apache.logging.log4j.Logger;
import utilities.JSONDesrializer;
import utilities.NodeTimer;
import utilities.PacketHandler;
import utilities.Strings;

import java.io.IOException;
import java.net.Socket;
import java.util.regex.Pattern;

/**
 * Responsible for holding common functions to use by Producer/Consumer.
 *
 * @author Palak Jain
 */
public class Client {
    protected Logger logger;
    protected volatile boolean isConnected;
    protected Host broker;
    protected Host loadBalancer;
    protected Connection connection;
    protected HostService hostService;
    protected String hostName;

    public Client(Logger logger, Properties properties) {
        this.logger = logger;

        String brokerInfo = properties.getValue(Constants.PROPERTY_KEY.BROKER);
        String loadBalancerInfo = properties.getValue(Constants.PROPERTY_KEY.LOADBALANCER);
        hostName = properties.getValue(Constants.PROPERTY_KEY.HOST_NAME);

        broker = getHostInfo(brokerInfo);
        loadBalancer = getHostInfo(loadBalancerInfo);
        hostService = new HostService(logger);
    }

    /**
     * Get the broker information which is holding the given topic and partition number
     */
    protected boolean getBroker(byte[] packet, String topic, int partitionNum) {
        boolean isSuccess = false;

        Partition partition = getBroker(packet);

        if (partition != null && partition.getLeader() != null && partition.getLeader().isValid()) {
            broker = partition.getLeader();
            isSuccess = true;
            logger.info(String.format("[%s] Received broker information: %s:%d which is holding the information of topic %s - partition %d.", hostName, broker.getAddress(), broker.getPort(), topic, partitionNum));
        } else {
            logger.warn(String.format("[%s] No broker information found which is holding the information of topic %s - partition %d.", hostName, topic, partitionNum));
        }

        return isSuccess;
    }

    /**
     * Get the broker information from the load balancer which is holding the partition information of a topic
     */
    protected Partition getBroker(byte[] packet) {
        Partition partition = null;

        try {
            Socket socket = new Socket(loadBalancer.getAddress(), loadBalancer.getPort());
            logger.info(String.format("[%s] [%s:%d] Successfully connected to the destination.", hostName, loadBalancer.getAddress(), loadBalancer.getPort()));

            Connection connection = new Connection(socket, loadBalancer.getAddress(), loadBalancer.getPort());
            if (connection.openConnection()) {
                NodeTimer timer = new NodeTimer();
                boolean running = true;

                connection.send(packet);
                timer.startTimer(Constants.TYPE.REQ.name(), Constants.RTT);

                while (running) {
                    if (timer.isTimeout()) {
                        logger.warn(String.format("[%s] [%s:%d] Time-out happen for the REQ packet to the host. Re-sending the packet.", hostName, connection.getDestinationIPAddress(), connection.getDestinationPort()));
                        connection.send(packet);
                        timer.stopTimer();
                        timer.startTimer(Constants.TYPE.REQ.name(), Constants.RTT);
                    } else if (connection.isAvailable()) {
                        byte[] responseBytes = connection.receive();

                        if (responseBytes != null) {
                            Header.Content header = PacketHandler.getHeader(responseBytes);

                            if (header != null) {
                                if (header.getType() == Constants.TYPE.RESP.getValue()) {
                                    logger.info(String.format("[%s] [%s:%d] Received the response for the REQ request from the host.", hostName, connection.getDestinationIPAddress(), connection.getDestinationPort()));
                                    timer.stopTimer();

                                    byte[] body = PacketHandler.getData(responseBytes);

                                    if (body != null) {
                                        running = false;
                                        partition = JSONDesrializer.fromJson(body, Partition.class);
                                    } else {
                                        logger.warn(String.format("[%s] [%s:%d] Received empty body from the load balancer. Retrying.", hostName, connection.getDestinationIPAddress(), connection.getDestinationPort()));
                                        connection.send(packet);
                                        timer.startTimer(Constants.TYPE.REQ.name(), Constants.RTT);
                                    }
                                } else if (header.getType() == Constants.TYPE.NACK.getValue()) {
                                    logger.warn(String.format("[%s] [%s:%d] Received negative acknowledgment for the REQ request from the host. Not retrying.", hostName, connection.getDestinationIPAddress(), connection.getDestinationPort()));
                                    timer.stopTimer();
                                    running = false;
                                }
                            }
                        }
                    }
                }
            }
        } catch (IOException exception) {
            logger.error(String.format("[%s:%d] Fail to make connection with the destination.", loadBalancer.getAddress(), loadBalancer.getPort()), exception.getMessage());
        }

        return partition;
    }

    /**
     * Send initial connection establishment request to the broker
     */
    protected boolean connectToBroker(byte[] packet, String packetName) {
        boolean isSuccess = false;

        try {
            Socket socket = new Socket(broker.getAddress(), broker.getPort());
            logger.info(String.format("[%s:%d] Successfully connected to the broker.", broker.getAddress(), broker.getPort()));

            connection = new Connection(socket, broker.getAddress(), broker.getPort());
            if (connection.openConnection()) {
                isSuccess = hostService.sendPacketWithACK(connection, packet, packetName);
            }
        } catch (IOException exception) {
            logger.error(String.format("[%s:%d] Fail to make connection with the broker.", broker.getAddress(), broker.getPort()), exception.getMessage());
        }

        return isSuccess;
    }

    /**
     * Checks if the given string contains numeric value or not
     */
    //cite: https://www.baeldung.com/java-check-string-number
    protected boolean isNumeric(String strNum) {
        if (strNum == null) {
            return false;
        }

        Pattern pattern = Pattern.compile("\\d+");
        return pattern.matcher(strNum).matches();
    }

    /**
     * Extract address and port information from the string
     */
    private Host getHostInfo(String detail) {
        Host host = null;

        if (!Strings.isNullOrEmpty(detail)) {
            String[] parts = detail.split(":");
            if (parts.length == 2 && isNumeric(parts[1])) {
                host = new Host(parts[0], Integer.parseInt(parts[1]));
            }
        }

        return host;
    }
}
