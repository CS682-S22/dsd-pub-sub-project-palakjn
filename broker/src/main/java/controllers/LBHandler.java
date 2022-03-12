package controllers;

import models.Host;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.Socket;

public class LBHandler {
    private static final Logger logger = (Logger) LogManager.getLogger(LBHandler.class);

    public void join(Host brokerInfo, Host loadBalancerInfo) {
        //Connect to the load balancer
        try {
            Socket socket = new Socket(loadBalancerInfo.getAddress(), loadBalancerInfo.getPort());
            logger.info(String.format("[%s:%d] Successfully connected to load balancer %s: %d.", brokerInfo.getAddress(), brokerInfo.getPort(), loadBalancerInfo.getAddress(), loadBalancerInfo.getPort()));

            Connection connection = new Connection(socket, loadBalancerInfo.getAddress(), loadBalancerInfo.getPort(), brokerInfo.getAddress(), brokerInfo.getPort());
            if (connection.openConnection()) {
                //Create join packet
                //Send the join packet
                //Start timer
                //start While loop
                //if timeout happen then, re-send the packet and re-start the timer
                //else check connection.available > 0
                // if true then read the response:
                // stop the timer
                // running = false
                //    If ACK then log and return true
                //    If NACK then log and return false
            }
        } catch (IOException exception) {
            logger.error(String.format("[%s:%d] Fail to make connection with the load balancer %s:%d. ", brokerInfo.getAddress(), brokerInfo.getPort(), loadBalancerInfo.getAddress(), loadBalancerInfo.getPort()), exception);
        }
    }
}
