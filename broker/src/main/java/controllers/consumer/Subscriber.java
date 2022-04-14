package controllers.consumer;

import configurations.BrokerConstants;
import controllers.Connection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.BrokerPacketHandler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Responsible for maintaining the subscriber information.
 *
 * @author Palak Jain
 */
public class Subscriber implements ISubscriber {
    private static final Logger logger = LogManager.getLogger(Subscriber.class);
    private Connection connection;
    private BlockingQueue<byte[]> queue;
    private Thread thread;

    public Subscriber(Connection connection) {
        this.connection = connection;
        queue = new LinkedBlockingDeque<>();

        thread = new Thread(this::send);
        thread.start();
    }

    /**
     * Gets the address of the subscriber
     */
    public String getAddress() {
        return String.format("%s:%d", connection.getDestinationIPAddress(), connection.getDestinationPort());
    }

    /**
     * Adding data to the queue to send to the subscriber
     */
    @Override
    public synchronized void onEvent(byte[] data) {
        byte[] packet = BrokerPacketHandler.createDataPacket(data);
        try {
            queue.put(packet);
        } catch (InterruptedException e) {
            logger.error("Unable to add an item to the queue", e);
        }
    }

    /**
     * Getting data from the queue and send it to the subscriber.
     */
    private void send() {
        while (connection.isOpen()) {
            try {
                byte[] data = queue.poll(BrokerConstants.PRODUCER_WAIT_TIME, TimeUnit.MILLISECONDS);

                if (data != null) {
                    connection.send(data);
                }
            } catch (InterruptedException e) {
                logger.error("Interrupted while getting data to send to the subscriber", e);
            }
        }
    }

    /**
     * Shutdown the subscriber
     */
    private void unSubscribe() {
        try {
            thread.join();
        } catch (InterruptedException exception) {
            logger.error("Interrupted while removing subscriber", exception);
        }
    }
}
