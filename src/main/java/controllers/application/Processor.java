package controllers.application;

import framework.Consumer;
import framework.Producer;

import configuration.Constants;
import configurations.AppConstants;
import configurations.Config;
import configurations.TopicConfig;
import controllers.Connection;
import models.Header;
import models.Properties;
import models.Topic;
import models.requests.CreateTopicRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utilities.AppPacketHandler;
import utilities.NodeTimer;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Responsible for producing/consuming logs from the Producer/Consumer based on the actions specified by the config
 *
 * @author Palak Jain
 */
public class Processor {
    private static final Logger logger = LogManager.getLogger(Processor.class);
    private ExecutorService threadPool;

    /**
     * Process the actions as instructed in the config
     */
    public void process(Config config) {
        if (config.isCreateTopic()) {
            createTopics(config);
        } else if (config.isProducer() || config.isConsumer()) {
            threadPool = Executors.newFixedThreadPool(config.getTopics().size());

            for (TopicConfig topicConfig : config.getTopics()) {
                if (topicConfig.isValid()) {
                    if (config.isProducer()) {
                        threadPool.execute(() -> produce(config, topicConfig));
                    } else {
                        threadPool.execute(() -> consume(config, topicConfig));
                    }
                }
            }
        }
    }

    /**
     * Create number of topics as given
     */
    private void createTopics(Config config) {
        //Connect to the load balancer

        try {
            Socket socket = new Socket(config.getLoadBalancer().getAddress(), config.getLoadBalancer().getPort());
            logger.info(String.format("[%s:%d] Successfully connected to the load balancer.", config.getLoadBalancer().getAddress(), config.getLoadBalancer().getPort()));

            Connection connection = new Connection(socket, config.getLoadBalancer().getAddress(), config.getLoadBalancer().getPort());
            if (connection.openConnection()) {
                List<CreateTopicRequest> topics = config.getTopicsToCreate();
                NodeTimer timer = new NodeTimer();
                int seqNum = 0; //Sequence number of the packet which is indented to send to another host

                for (CreateTopicRequest createTopicRequest : topics) {
                    logger.info(String.format("[%s:%d] Creating %d partitions of topic %s.", config.getLoadBalancer().getAddress(), config.getLoadBalancer().getPort(), createTopicRequest.getNumOfPartitions(), createTopicRequest.getName()));

                    byte[] packet = AppPacketHandler.createAddTopicPacket(createTopicRequest, seqNum);

                    connection.send(packet);
                    timer.startTimer("TOPIC", AppConstants.RTT);
                    boolean reading = true;

                    while (reading) {
                        if (timer.isTimeout()) {
                            logger.warn(String.format("[%s:%d] Timeout happen for the packet having topic %s - partition count %d information. Sending again", config.getLoadBalancer().getAddress(), config.getLoadBalancer().getPort(), createTopicRequest.getName(), createTopicRequest.getNumOfPartitions()));
                            connection.send(packet);
                            timer.stopTimer();
                            timer.startTimer("TOPIC", AppConstants.RTT);
                        } else if (connection.isAvailable()) {
                            byte[] responseInBytes = connection.receive();

                            if (responseInBytes != null) {
                                Header.Content header = AppPacketHandler.getHeader(responseInBytes);

                                if (header != null) {
                                    if (header.getType() == AppConstants.TYPE.ACK.getValue()) {
                                        if (header.getSeqNum() == seqNum) {
                                            logger.info(String.format("[%s:%d] Received an acknowledgment for the packet with the sequence number %d.", config.getLoadBalancer().getAddress(), config.getLoadBalancer().getPort(), seqNum));
                                            reading = false;
                                            seqNum++;
                                            timer.stopTimer();
                                        } else if (header.getSeqNum() < seqNum) {
                                            logger.info(String.format("[%s:%d] Received an acknowledgment for the older packet with the sequence number %d. Ignoring it.", config.getLoadBalancer().getAddress(), config.getLoadBalancer().getPort(), header.getSeqNum()));
                                        }
                                    } else if (header.getType() == AppConstants.TYPE.NACK.getValue()) {
                                        logger.info(String.format("[%s:%d] Received a negative acknowledgment for the packet having topic %s - partition count %d information.", config.getLoadBalancer().getAddress(), config.getLoadBalancer().getPort(), createTopicRequest.getName(), createTopicRequest.getNumOfPartitions()));
                                        reading = false;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (IOException exception) {
            logger.error(String.format("[%s:%d] Fail to make connection with the load balancer.", config.getLoadBalancer().getAddress(), config.getLoadBalancer().getPort()), exception);
        }
    }

    /**
     * Reads from the logs and send each log line to the producer
     */
    private void produce(Config config, TopicConfig topicConfig) {
        String hostName = String.format("framework.Producer - %s", config.getHostName());
        logger.info(String.format("Started %s", hostName));

        Properties properties = new Properties();
        properties.put(AppConstants.PROPERTY_KEY.LOADBALANCER, String.format("%s:%d", config.getLoadBalancer().getAddress(), config.getLoadBalancer().getPort()));
        //Below is optional. For logging purpose.
        properties.put(AppConstants.PROPERTY_KEY.HOST_NAME, hostName);

        Producer producer = new Producer(properties);

        int count = 0; //For logging purpose
        try (BufferedReader reader = new BufferedReader(new FileReader(topicConfig.getLocation()))) {
            String line = reader.readLine();

            while (line != null) {
                producer.send(topicConfig.getName(), topicConfig.getKey(), line.getBytes(StandardCharsets.UTF_8));
                line = reader.readLine();

                count++;
            }

            logger.info(String.format("[%s] Send %d number of data for topic %s - partition %d", hostName, count, topicConfig.getName(), topicConfig.getKey()));
        } catch (IOException exception) {
            logger.error(String.format("[%s] Unable to open the file at the location %s.", hostName, topicConfig.getLocation()), exception);
        }
    }

    /**
     * Fetch logs from the consumer in a specified amount of time
     */
    private void consume(Config config, TopicConfig topicConfig) {
        String hostName = String.format("framework.Consumer - %s", config.getHostName());
        logger.info(String.format("Started %s", hostName));

        String method = AppConstants.METHOD.PULL.name();
        if (config.getMethod() == Constants.METHOD.PUSH.getValue()) {
            method = AppConstants.METHOD.PUSH.name();
        }

        Properties properties = new Properties();
        properties.put(AppConstants.PROPERTY_KEY.LOADBALANCER, String.format("%s:%d", config.getLoadBalancer().getAddress(), config.getLoadBalancer().getPort()));
        properties.put(AppConstants.PROPERTY_KEY.METHOD, method);
        properties.put(AppConstants.PROPERTY_KEY.OFFSET, topicConfig.getOffset());
        //Below is optional. For logging purpose.
        properties.put(AppConstants.PROPERTY_KEY.HOST_NAME, hostName);

        Consumer consumer = new Consumer(properties);

        //Subscribing to the topic
        if (consumer.subscribe(topicConfig.getName(), topicConfig.getKey())) {
            String fileLocation = String.format("%s/%s_%d.log", topicConfig.getLocation(), topicConfig.getName(), topicConfig.getKey());

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileLocation, true))) {
                while (true) {
                    byte[] data = consumer.poll(AppConstants.WAIT_TIME);

                    if (data != null) {
                        String string = new String(data);
                        writer.write(string);
                        writer.newLine();
                        writer.flush();
                    }
                }
            } catch (IOException exception) {
                logger.error(String.format("[%s] Unable to open/create the file at the location %s.", hostName, fileLocation), exception);
            }
        }
    }
}
