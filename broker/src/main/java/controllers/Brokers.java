package controllers;

import configurations.BrokerConstants;
import models.Host;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Responsible for holding all brokers information holding particular topic-partition.
 *
 * @author Palak Jain
 */
public class Brokers {
    private List<Broker> brokers;
    private ReentrantReadWriteLock lock;

    public Brokers() {
        brokers = new ArrayList<>();
        lock = new ReentrantReadWriteLock();
    }

    public Brokers(List<Host> brokers) {
        this.brokers = new ArrayList<>();
        for (Host host : brokers) {
            this.brokers.add(new Broker(host));
        }

        lock = new ReentrantReadWriteLock();
    }

    /**
     * Add new broker
     */
    public void add(Host host) {
        lock.writeLock().lock();

        brokers.add(new Broker(host));

        lock.writeLock().unlock();
    }

    /**
     * Remove the broker
     */
    public void remove(Host host) {
        lock.writeLock().lock();

        if (brokers.size() > 0 && contains(host)) {
            Broker broker = getBroker(host);
            broker.close();
            brokers.removeIf(broker1 -> host.getAddress().equals(broker1.getAddress()) && host.getPort() == broker1.getPort());
        }

        lock.writeLock().unlock();
    }

    /**
     * Get the number of brokers
     */
    public int getSize() {
        lock.readLock().lock();

        try {
            return brokers.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the broker
     */
    public Broker getBroker(Host host) {
        Broker broker = null;
        lock.readLock().lock();

        for (Broker bro : brokers) {
            if (bro.equals(host)) {
                broker = new Broker(bro);
            }
        }

        lock.readLock().unlock();
        return broker;
    }

    /**
     * Send the data to all the other brokers
     *
     * @return true if success to send the data to all the other brokers else false if not able to send the data to one or more brokers.
     */
    public boolean send(byte[] data, BrokerConstants.CHANNEL_TYPE channel_type, int waitTime, boolean retry) {
        boolean isSuccess = true;
        lock.readLock().lock();

        //Sending the data to all the other brokers. Even when one of the broker failed.
        for (Broker broker : brokers) {
            isSuccess = broker.send(data, channel_type, waitTime, retry) && isSuccess;
        }

        lock.readLock().unlock();
        return isSuccess;
    }

    /**
     * Checks whether the given host exists in the collection
     */
    public boolean contains(Host broker) {
        lock.readLock().lock();

        try {
            return brokers.stream().anyMatch(host -> host.getAddress().equals(broker.getAddress()) && host.getPort() == broker.getPort());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the list of brokers which has high or less priority number (based on given choice) than the given one
     */
    public List<Broker> getBrokers(int priorityNum, BrokerConstants.PRIORITY_CHOICE priority_choice) {
        List<Broker> collection = new ArrayList<>();
        lock.readLock().lock();

        for (Broker broker : brokers) {
            if ((priority_choice == BrokerConstants.PRIORITY_CHOICE.HIGH && broker.getPriorityNum() > priorityNum) ||
                    (priority_choice == BrokerConstants.PRIORITY_CHOICE.LESS && broker.getPriorityNum() < priorityNum)) {
                collection.add(broker);
            }
        }

        lock.readLock().unlock();
        return collection;
    }

    /**
     * Get the list of brokers
     */
    public List<Broker> getBrokers() {
        lock.readLock().lock();

        try {
            return brokers;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the list of brokers which has outOfSync data
     */
    public List<Broker> getOutOfSyncBrokers() {
        List<Broker> collection = new ArrayList<>();
        lock.readLock().lock();

        for (Broker broker : brokers) {
            if (broker.isOutOfSync()) {
                collection.add(broker);
            }
        }

        lock.readLock().unlock();
        return collection;
    }
}
