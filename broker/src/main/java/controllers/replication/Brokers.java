package controllers.replication;

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
            brokers.remove(broker);
        }

        lock.writeLock().unlock();
    }

    /**
     * Get the broker
     */
    public Broker getBroker(Host host) {
        lock.readLock().lock();

        try {
            return (Broker) brokers.stream().filter(broker -> broker.getAddress().equals(host.getAddress()) && broker.getPort() == host.getPort());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Send the data to all the other brokers
     *
     * @return true if success to send the data to all the other brokers else false if not able to send the data to one or more brokers.
     */
    public boolean send(byte[] data) {
        boolean isSuccess = true;
        lock.readLock().lock();

        //Sending the data to all the other brokers. Even when one of the broker failed.
        for (Broker broker : brokers) {
            isSuccess = broker.send(data) && isSuccess;
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
}
