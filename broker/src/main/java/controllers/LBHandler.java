package controllers;

import configuration.Constants;
import models.Header;
import models.Host;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import utilities.BrokerPacketHandler;
import utilities.NodeTimer;

import java.io.IOException;
import java.net.Socket;

public class LBHandler {
    private static final Logger logger = (Logger) LogManager.getLogger(LBHandler.class);

    public boolean join(Host brokerInfo, Host loadBalancerInfo) {
        return send(brokerInfo, loadBalancerInfo, Constants.TYPE.ADD);
    }

    public boolean remove(Host brokerInfo, Host loadBalancerInfo) {
        return send(brokerInfo, loadBalancerInfo, Constants.TYPE.REM);
    }

    private boolean send(Host brokerInfo, Host loadBalancerInfo, Constants.TYPE type) {
        boolean isSuccess = false;

        try {
            Socket socket = new Socket(loadBalancerInfo.getAddress(), loadBalancerInfo.getPort());
            logger.info(String.format("[%s:%d] Successfully connected to load balancer %s: %d.", brokerInfo.getAddress(), brokerInfo.getPort(), loadBalancerInfo.getAddress(), loadBalancerInfo.getPort()));

            Connection connection = new Connection(socket, loadBalancerInfo.getAddress(), loadBalancerInfo.getPort(), brokerInfo.getAddress(), brokerInfo.getPort());
            if (connection.openConnection()) {
                NodeTimer timer = new NodeTimer();
                boolean running = true;
                byte[] packet = BrokerPacketHandler.createPacket(brokerInfo, type);

                connection.send(packet);
                timer.startTimer(type.name());

                while (running) {
                    if (timer.isTimeout()) {
                        logger.warn(String.format("[%s:%d] Time-out happen for the packet %s to the load balancer %s:%d. Re-sending the packet.", brokerInfo.getAddress(), brokerInfo.getPort(), type.name(), loadBalancerInfo.getAddress(), loadBalancerInfo.getPort()));
                        connection.send(packet);
                        timer.startTimer(type.name());
                    } else if (connection.isAvailable()) {
                        byte[] responseBytes = connection.receive();

                        if (responseBytes != null) {
                            Header.Content header = BrokerPacketHandler.getHeader(responseBytes);

                            if (header != null && header.getRequester() == Constants.REQUESTER.LOAD_BALANCER.getValue()) {
                                if (header.getType() == Constants.TYPE.ACK.getValue()) {
                                    logger.info(String.format("[%s:%d] Received an acknowledgment for the %s request from the load balancer %s:%d.", brokerInfo.getAddress(), brokerInfo.getPort(), type.name(), loadBalancerInfo.getAddress(), loadBalancerInfo.getPort()));
                                    timer.stopTimer();
                                    running = false;
                                    isSuccess = true;
                                } else if (header.getType() == Constants.TYPE.NACK.getValue()) {
                                    logger.warn(String.format("[%s:%d] Received negative acknowledgment for the %s request from the load balancer %s:%d. Not retrying.", brokerInfo.getAddress(), brokerInfo.getPort(), type.name(), loadBalancerInfo.getAddress(), loadBalancerInfo.getPort()));
                                    timer.stopTimer();
                                    running = false;
                                }
                            }
                        }
                    }
                }
            }
        } catch (IOException exception) {
            logger.error(String.format("[%s:%d] Fail to make connection with the load balancer %s:%d. ", brokerInfo.getAddress(), brokerInfo.getPort(), loadBalancerInfo.getAddress(), loadBalancerInfo.getPort()), exception);
        }

        return isSuccess;
    }
}
