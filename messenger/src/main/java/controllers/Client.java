package controllers;

import configuration.Constants;
import models.Header;
import models.Host;
import models.Properties;
import models.responses.Response;
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

        broker = getBroker(packet);

        if (broker != null && broker.isValid()) {
            isSuccess = true;
            logger.info(String.format("[%s] Received broker information: %s:%d which is holding the information of topic %s - partition %d.", hostName, broker.getAddress(), broker.getPort(), topic, partitionNum));
        } else {
            logger.warn(String.format("[%s] No broker information found which is holding the information of topic %s - partition %d.", hostName, topic, partitionNum));
            broker = null;
        }

        return isSuccess;
    }

    /**
     * Get the broker information from the load balancer which is holding the partition information of a topic
     */
    protected Host getBroker(byte[] packet) {
        Host broker = null;

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

                                        Response<Host> response = JSONDesrializer.fromJson(body, Response.class);

                                        if (response != null && response.isValid()) {
                                            if (response.isInSync()) {
                                                logger.warn(String.format("[%s] [%s:%d] Received response with status as SYNC from load balancer. Sleeping for %d amount of time and retrying.", hostName, connection.getDestinationIPAddress(), connection.getDestinationPort(), Constants.GET_BROKER_WAIT_TIME));
                                                Thread.sleep(Constants.GET_BROKER_WAIT_TIME);
                                                connection.send(packet);
                                                timer.startTimer(Constants.TYPE.REQ.name(), Constants.RTT);
                                            } else if (response.isOk()) {
                                                broker = response.getObject();
                                            } else {
                                                logger.warn(String.format("[%s] [%s:%d] Received response with invalid status %s from the load balancer. Retrying.", hostName, connection.getDestinationIPAddress(), connection.getDestinationPort(), response.getStatus()));
                                                connection.send(packet);
                                                timer.startTimer(Constants.TYPE.REQ.name(), Constants.RTT);
                                            }
                                        } else {
                                            logger.warn(String.format("[%s] [%s:%d] Received invalid response from the load balancer. Retrying.", hostName, connection.getDestinationIPAddress(), connection.getDestinationPort()));
                                            connection.send(packet);
                                            timer.startTimer(Constants.TYPE.REQ.name(), Constants.RTT);
                                        }
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
            logger.error(String.format("[%s:%d] Fail to get broker information from load balancer.", loadBalancer.getAddress(), loadBalancer.getPort()), exception.getMessage());
        } catch (InterruptedException e) {
            logger.error(String.format("[%s:%d] Interrupted while getting broker information from load balancer.", loadBalancer.getAddress(), loadBalancer.getPort()), e.getMessage());
        }

        return broker;
    }

    /**
     * Send initial connection establishment request to the broker
     */
    protected boolean connectToBroker(byte[] packet, String packetName) {
        boolean isSuccess = false;

        connection = hostService.connect(broker.getAddress(), broker.getPort());

        if (connection != null) {
            isSuccess = hostService.sendPacketWithACK(connection, packet, packetName);
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
