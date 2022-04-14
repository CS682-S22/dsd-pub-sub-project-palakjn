package configurations;

import models.Host;

/**
   Responsible for holding broker config values

   @author Palak Jain
 */
public class Config {
    private Host local;
    private Host heartBeatServer;
    private Host loadBalancer;

    /**
     * @return Details of the host which is running locally
     */
    public Host getLocal() {
        return local;
    }

    /**
     * @return Details of the host which is running locally to listen for heartbeat message
     */
    public Host getHeartBeatServer() {
        return heartBeatServer;
    }

    /**
     * @return Details of the host which is load balancer
     */
    public Host getLoadBalancer() {
        return loadBalancer;
    }

    /**
     * @return true if the config contains required values else false
     */
    public boolean isValid() {
        return !(local == null ||
                !local.isValid() ||
                loadBalancer == null ||
                !loadBalancer.isValid());
    }
}
