import configurations.BrokerConstants;
import configurations.Config;
import controllers.Connection;
import controllers.database.CacheManager;
import controllers.loadBalancer.LBHandler;
import controllers.RequestHandler;
import models.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.JSONDesrializer;
import utilities.Strings;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Responsible for starting the instance of the Broker to the configured host.
 *
 * @author Palak Jain
 */
public class Broker {
    private static final Logger logger = LogManager.getLogger(Broker.class);
    private ExecutorService threadPool;
    private boolean running;

    public Broker() {
        threadPool = Executors.newFixedThreadPool(BrokerConstants.NUM_OF_THREADS);
        running = true;
    }

    public static void main(String[] args) {
        Broker broker = new Broker();
        String location = broker.getConfigLocation(args);

        if (!Strings.isNullOrEmpty(location)) {
            Config config = broker.getConfig(location);

            if (broker.isValid(config)) {
                CacheManager.setBroker(new Host(config.getLocal().getAddress(), config.getLocal().getPort()));

                //Joining to the network
                LBHandler lbHandler = new LBHandler(config.getLoadBalancer());
                if (lbHandler.join()) {
                    //Starting thread to listen for request/response from load balancer
                    Thread lbThread = new Thread(lbHandler::listen);
                    lbThread.start();

                    logger.info(String.format("[%s] Listening on DATA/SYNC port %d.", config.getLocal().getAddress(), config.getLocal().getPort()));
                    System.out.printf("[%s] Listening on DATA/SYNC port %d.\n", config.getLocal().getAddress(), config.getLocal().getPort());

                    //Starting thread to listen for the DATA/SYNC connections
                    Thread dataConnectionThread = new Thread(() -> broker.listen(config.getLocal().getAddress(), config.getLocal().getPort()));
                    dataConnectionThread.start();

                    //Starting thread to listen for the HEARTBEAT/ELECTION connections
                    logger.info(String.format("[%s] Listening on HEARTBEAT/ELECTION port %d.", config.getHeartBeatServer().getAddress(), config.getHeartBeatServer().getPort()));
                    System.out.printf("[%s] Listening on HEARTBEAT/ELECTION port %d.\n", config.getHeartBeatServer().getAddress(), config.getHeartBeatServer().getPort());

                    Thread heartBeatThread = new Thread(() -> broker.listen(config.getHeartBeatServer().getAddress(), config.getHeartBeatServer().getPort()));
                    heartBeatThread.start();
                }
            }
        }
    }

    /**
     * Get the location of the config file from arguments
     */
    private String getConfigLocation(String[] args) {
        String location = null;

        if (args.length == 2 &&
            args[0].equalsIgnoreCase("-config") &&
            !Strings.isNullOrEmpty(args[1])) {
            location = args[1];
        } else {
            System.out.println("Invalid Arguments");
        }

        return location;
    }

    /**
     * Read and De-Serialize the config file from the given location
     */
    private Config getConfig(String location) {
        Config config = null;

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(location))){
            config = JSONDesrializer.fromJson(reader, Config.class);
        }
        catch (IOException ioException) {
            System.out.printf("Unable to open configuration file at location %s. %s. \n", location, ioException.getMessage());
        }

        return config;
    }

    /**
     * Validates whether the config contains the required values or not
     */
    private boolean isValid(Config config) {
        boolean flag = false;

        if (config == null) {
            System.out.println("No configuration found.");
        } else if (!config.isValid()) {
            System.out.println("Invalid values found in the configuration file.");
        } else {
            flag = true;
        }

        return flag;
    }

    /**
     * Listen for DATA/SYNC connection
     */
    private void listen(String address, int port) {
        ServerSocket serverSocket;

        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException exception) {
            logger.error(String.format("Fail to start the broker at the node %s: %d.", address, port), exception);
            return;
        }

        while (running) {
            try {
                Socket socket = serverSocket.accept();
                logger.info(String.format("[%s:%d] Received the connection from the host.", socket.getInetAddress().getHostAddress(), socket.getPort()));
                Connection connection = new Connection(socket, socket.getInetAddress().getHostAddress(), socket.getPort(), address, port);
                if (connection.openConnection()) {
                    RequestHandler requestHandler = new RequestHandler(connection);
                    threadPool.execute(requestHandler::process);
                }
            } catch (IOException exception) {
                logger.error(String.format("[%s:%d] Fail to accept the connection from another host. ", address, port), exception);
            }
        }
    }
}
