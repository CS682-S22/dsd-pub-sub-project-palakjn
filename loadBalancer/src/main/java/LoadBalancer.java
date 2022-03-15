import configuration.Constants;
import configurations.Config;
import controllers.Connection;
import controllers.RequestHandler;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LoadBalancer {
    private static final Logger logger = LogManager.getLogger(LoadBalancer.class);
    private ExecutorService threadPool;
    Config config;
    private boolean running = true;

    public LoadBalancer(Config config) {
        this.threadPool = Executors.newFixedThreadPool(Constants.NUM_OF_THREADS);
        this.config = config;
    }

    public static void main(String[] args) {

    }

    private void listen() {
        ServerSocket serverSocket;

        try {
            serverSocket = new ServerSocket(config.getPort());
        } catch (IOException exception) {
            logger.error(String.format("Fail to start the load balancer at the node %s: %d.", config.getAddress(), config.getPort()), exception);
            return;
        }

        logger.info(String.format("[%s] Listening on port %d.", config.getAddress(), config.getPort()));
        System.out.printf("[%s] Listening on port %d.\n", config.getAddress(), config.getPort());

        while (running) {
            try {
                Socket socket = serverSocket.accept();
                logger.info(String.format("[%s:%d] Received the connection from the host.", socket.getInetAddress().getHostAddress(), socket.getPort()));
                Connection connection = new Connection(socket, socket.getInetAddress().getHostAddress(), socket.getPort(), config.getAddress(), config.getPort());
                if (connection.openConnection()) {
                    RequestHandler requestHandler = new RequestHandler(connection);
                    threadPool.execute(requestHandler::process);
                }
            } catch (IOException exception) {
                logger.error(String.format("[%s:%d] Fail to accept the connection from another host. ", config.getAddress(), config.getPort()), exception);
            }
        }
    }
}
