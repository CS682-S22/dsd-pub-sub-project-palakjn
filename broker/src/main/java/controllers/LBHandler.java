package controllers;

import configuration.Constants;
import models.Host;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import utilities.BrokerPacketHandler;

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
                HostService service = new HostService(connection, logger);
                byte[] packet = BrokerPacketHandler.createPacket(brokerInfo, type);
                isSuccess = service.sendPacketWithACK(packet, String.format("%s:%s", Constants.REQUESTER.BROKER.name(), type.name()));
            }
        } catch (IOException exception) {
            logger.error(String.format("[%s:%d] Fail to make connection with the load balancer %s:%d. ", brokerInfo.getAddress(), brokerInfo.getPort(), loadBalancerInfo.getAddress(), loadBalancerInfo.getPort()), exception);
        }

        return isSuccess;
    }
}
