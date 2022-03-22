package models;

import controllers.Subscriber;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A thread-safe data structure which will hold Subscribers object.
 *
 * @author Palak Jain
 */
public class Subscribers {
    private List<Subscriber> subscribers;
    private ReentrantReadWriteLock lock;

    public Subscribers(){
        this.subscribers = new ArrayList<>();
        this.lock = new ReentrantReadWriteLock();
    }

    /**
     * Adding subscribeHandler to the list.
     * @param subscriber The one who wants to subscribe.
     */
    public void add(Subscriber subscriber) {
        this.lock.writeLock().lock();

        try {
            this.subscribers.add(subscriber);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    /**
     * Gets the total number of subscribeHandlers.
     * @return size of an array
     */
    public int size() {
        this.lock.readLock().lock();

        try {
            return this.subscribers.size();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    /**
     * Gets the subscriber object at a given index
     * @param index location of an item in an array
     * @return SubscribeHandler object if a given index is less than the size of an array else null
     */
    public Subscriber get(int index) {
        this.lock.readLock().lock();

        try {
            if(index < this.subscribers.size()) {
                return subscribers.get(index);
            }
            else {
                return null;
            }
        } finally {
            this.lock.readLock().unlock();
        }
    }
}
