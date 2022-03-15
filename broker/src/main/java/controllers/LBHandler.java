package controllers;

import configurations.BrokerConstants;
import models.Header;
import models.Host;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import utilities.BrokerPacketHandler;

import java.io.IOException;
import java.net.Socket;

public class LBHandler {
    private static final Logger logger = LogManager.getLogger(LBHandler.class);

    public boolean join(Host brokerInfo, Host loadBalancerInfo) {
        return send(brokerInfo, loadBalancerInfo, BrokerConstants.TYPE.ADD);
    }

    public boolean remove(Host brokerInfo, Host loadBalancerInfo) {
        return send(brokerInfo, loadBalancerInfo, BrokerConstants.TYPE.REM);
    }

    public void processRequest(Header.Content header, byte[] request) {
        if (header.getType() == BrokerConstants.TYPE.ADD.getValue()) {
            //Adding new topic information

        }
    }

    private boolean send(Host brokerInfo, Host loadBalancerInfo, BrokerConstants.TYPE type) {
        boolean isSuccess = false;

        try {
            Socket socket = new Socket(loadBalancerInfo.getAddress(), loadBalancerInfo.getPort());
            logger.info(String.format("[%s:%d] Successfully connected to load balancer %s: %d.", brokerInfo.getAddress(), brokerInfo.getPort(), loadBalancerInfo.getAddress(), loadBalancerInfo.getPort()));

            Connection connection = new Connection(socket, loadBalancerInfo.getAddress(), loadBalancerInfo.getPort(), brokerInfo.getAddress(), brokerInfo.getPort());
            if (connection.openConnection()) {
                HostService service = new HostService(connection, logger);
                byte[] packet = BrokerPacketHandler.createPacket(brokerInfo, type);
                isSuccess = service.sendPacketWithACK(packet, String.format("%s:%s", BrokerConstants.REQUESTER.BROKER.name(), type.name()));
            }
        } catch (IOException exception) {
            logger.error(String.format("[%s:%d] Fail to make connection with the load balancer %s:%d. ", brokerInfo.getAddress(), brokerInfo.getPort(), loadBalancerInfo.getAddress(), loadBalancerInfo.getPort()), exception);
        }

        return isSuccess;
    }
}
