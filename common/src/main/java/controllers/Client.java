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

public class Client {
    protected Logger logger;
    protected boolean isConnected;
    protected Host broker;
    protected Host loadBalancer;
    protected Connection connection;
    protected HostService hostService;

    public Client(Logger logger, Properties properties) {
        this.logger = logger;

        String brokerInfo = properties.getValue(Constants.PROPERTY_KEY.BROKER);
        String loadBalancerInfo = properties.getValue(Constants.PROPERTY_KEY.LOADBALANCER);

        broker = getHostInfo(brokerInfo);
        loadBalancer = getHostInfo(loadBalancerInfo);
        hostService = new HostService(logger);
    }

    protected Partition getBroker(byte[] packet) {
        Partition partition = null;

        try {
            Socket socket = new Socket(loadBalancer.getAddress(), loadBalancer.getPort());
            logger.info(String.format("[%s:%d] Successfully connected to the destination.", loadBalancer.getAddress(), loadBalancer.getPort()));

            Connection connection = new Connection(socket, loadBalancer.getAddress(), loadBalancer.getPort());
            if (connection.openConnection()) {
                NodeTimer timer = new NodeTimer();
                boolean running = true;

                connection.send(packet);
                timer.startTimer(Constants.TYPE.REQ.name(), Constants.RTT);

                while (running) {
                    if (timer.isTimeout()) {
                        logger.warn(String.format("[%s:%d] Time-out happen for the REQ packet to the host. Re-sending the packet.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                        connection.send(packet);
                        timer.stopTimer();
                        timer.startTimer(Constants.TYPE.REQ.name(), Constants.RTT);
                    } else if (connection.isAvailable()) {
                        byte[] responseBytes = connection.receive();

                        if (responseBytes != null) {
                            Header.Content header = PacketHandler.getHeader(responseBytes);

                            if (header != null) {
                                if (header.getType() == Constants.TYPE.RESP.getValue()) {
                                    logger.info(String.format("[%s:%d] Received the response for the REQ request from the host.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                                    timer.stopTimer();

                                    byte[] body = PacketHandler.getData(responseBytes);

                                    if (body != null) {
                                        running = false;
                                        partition = JSONDesrializer.fromJson(body, Partition.class);
                                    } else {
                                        logger.warn(String.format("[%s:%d] Received empty body from the load balancer. Retrying.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
                                        connection.send(packet);
                                        timer.startTimer(Constants.TYPE.REQ.name(), Constants.RTT);
                                    }
                                } else if (header.getType() == Constants.TYPE.NACK.getValue()) {
                                    logger.warn(String.format("[%s:%d] Received negative acknowledgment for the REQ request from the host. Not retrying.", connection.getDestinationIPAddress(), connection.getDestinationPort()));
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

    //cite: https://www.baeldung.com/java-check-string-number
    private boolean isNumeric(String strNum) {
        if (strNum == null) {
            return false;
        }

        Pattern pattern = Pattern.compile("\\d+");
        return pattern.matcher(strNum).matches();
    }
}
