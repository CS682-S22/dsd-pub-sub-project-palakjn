import configurations.Config;
import configuration.Constants;
import controllers.Connection;
import controllers.LBHandler;
import controllers.RequestHandler;
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
    private boolean running = true;

    public Broker() {
        this.threadPool = Executors.newFixedThreadPool(Constants.NUM_OF_THREADS);
    }

    public static void main(String[] args) {
        Broker broker = new Broker();
        String location = broker.getConfigLocation(args);

        if (!Strings.isNullOrEmpty(location)) {
            Config config = broker.getConfig(location);

            if (broker.isValid(config)) {

                //Joining to the network
                LBHandler lbHandler = new LBHandler();
                if (lbHandler.join(config.getLocal(), config.getLoadBalancer())) {
                    //Listening for the new connections
                    broker.listen(config);
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
     * Listen for new connection
     */
    private void listen(Config config) {
        ServerSocket serverSocket;

        try {
            serverSocket = new ServerSocket(config.getLocal().getPort());
        } catch (IOException exception) {
            logger.error(String.format("Fail to start the broker at the node %s: %d.", config.getLocal().getAddress(), config.getLocal().getPort()), exception);
            return;
        }
        logger.info(String.format("[%s] Listening on port %d.", config.getLocal().getAddress(), config.getLocal().getPort()));
        System.out.printf("[%s] Listening on port %d.\n", config.getLocal().getAddress(), config.getLocal().getPort());

        while (running) {
            try {
                Socket socket = serverSocket.accept();
                logger.info(String.format("[%s:%d] Received the connection from the host.", socket.getInetAddress().getHostAddress(), socket.getPort()));
                Connection connection = new Connection(socket, socket.getInetAddress().getHostAddress(), socket.getPort(), config.getLocal().getAddress(), config.getLocal().getPort());
                if (connection.openConnection()) {
                    RequestHandler requestHandler = new RequestHandler(connection);
                    threadPool.execute(requestHandler::process);
                }
            } catch (IOException exception) {
                logger.error(String.format("[%s:%d] Fail to accept the connection from another host. ", config.getLocal().getAddress(), config.getLocal().getPort()), exception);
            }
        }
    }
}
