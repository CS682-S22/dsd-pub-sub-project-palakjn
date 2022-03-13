package controllers;

import configuration.Constants;
import models.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import utilities.JSONDesrializer;
import utilities.LBPacketHandler;

public class RequestHandler {
    private static final Logger logger = LogManager.getLogger(RequestHandler.class);
    private HostService hostService;
    private Connection connection;

    public RequestHandler(Connection connection) {
        this.connection = connection;
        this.hostService = new HostService(connection);
    }

    public void process() {
        byte[] request = connection.receive();

        if (request != null) {
            Header.Content header = LBPacketHandler.getHeader(request);

            if (header.getRequester() == Constants.REQUESTER.BROKER.getValue()) {
                processBrokerRequest(request, header.getType());
            } else if ((header.getRequester() == Constants.REQUESTER.PRODUCER.getValue() ||
                        header.getRequester() == Constants.REQUESTER.CONSUMER.getValue()) &&
                        header.getType() == Constants.TYPE.REQ.getValue()) {
                sendBrokerDetails(request);
            } else if (header.getRequester() == Constants.REQUESTER.TOPIC.getValue()) {
                processTopicRequest(request);
            }
        }

        //If header.requester == topic and header.type == add then, do the partition and add the topic to the collection
    }

    private void processBrokerRequest(byte[] message, int action) {

        byte[] body = LBPacketHandler.getData(message);

        if (body != null) {
            Host broker = JSONDesrializer.fromJson(message, Host.class);

            if (broker != null && broker.isValid()) {
                if (action == Constants.TYPE.ADD.getValue()) {
                    CacheManager.addBroker(broker);
                    hostService.sendACK(Constants.REQUESTER.LOAD_BALANCER);
                } else if (action == Constants.TYPE.REM.getValue()) {
                    CacheManager.removeBroker(broker);
                    hostService.sendACK(Constants.REQUESTER.LOAD_BALANCER);
                } else {
                    hostService.sendNACK(Constants.REQUESTER.LOAD_BALANCER);
                }
            } else {
                hostService.sendNACK(Constants.REQUESTER.LOAD_BALANCER);
            }
        }
    }

    private void sendBrokerDetails(byte[] message) {
        byte[] body = LBPacketHandler.getData(message);
        byte[] response = null;

        if (body != null) {
            Request request = JSONDesrializer.fromJson(body, Request.class);

            if (request != null && request.isValid()) {
                if (request.getType() == Constants.REQUEST.TOPIC.getValue()) {
                    //Requesting for the entire topic

                    if (CacheManager.isTopicExist(request.getTopicName())) {
                        Topic topic = CacheManager.getTopic(request.getTopicName());
                        response = LBPacketHandler.createResponse(topic);
                    } else {
                        //Topic don't exist
                        hostService.sendNACK(Constants.REQUESTER.LOAD_BALANCER);
                    }
                } else {
                    //Requesting for a partition
                    if (CacheManager.isPartitionExist(request.getTopicName(), request.getPartition())) {
                        Partition partition = CacheManager.getPartition(request.getTopicName(), request.getPartition());
                        response = LBPacketHandler.createResponse(partition);
                    } else {
                        //Partition don't exit
                        hostService.sendNACK(Constants.REQUESTER.LOAD_BALANCER);
                    }
                }
            } else {
                hostService.sendNACK(Constants.REQUESTER.LOAD_BALANCER);
            }
        }

        if (response != null) {
            connection.send(response);
        }
    }

    private void processTopicRequest(byte[] message) {

    }
}
