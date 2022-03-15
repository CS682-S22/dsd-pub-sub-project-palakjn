package configurations;

import models.Host;

public class Config {
    private Host local;
    private Host loadBalancer;

    public Host getLocal() {
        return local;
    }

    public Host getLoadBalancer() {
        return loadBalancer;
    }

    public boolean isValid() {
        return !(local == null ||
                !local.isValid() ||
                loadBalancer == null ||
                !loadBalancer.isValid());
    }
}
