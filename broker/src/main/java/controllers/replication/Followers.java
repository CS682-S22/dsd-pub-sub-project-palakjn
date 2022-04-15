package controllers.replication;

import models.Host;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Responsible for holding all the followers of the broker per topic-partition.
 *
 * @author Palak Jain
 */
public class Followers {
    private List<Follower> followers;
    private ReentrantReadWriteLock lock;

    public Followers() {
        followers = new ArrayList<>();
        lock = new ReentrantReadWriteLock();
    }

    /**
     * Add new follower
     */
    public void add(Host host) {
        lock.writeLock().lock();

        followers.add(new Follower(host));

        lock.writeLock().unlock();
    }

    /**
     * Remove the follower
     */
    public void remove(Host host) {
        lock.writeLock().lock();

        if (followers.size() > 0 && contains(host)) {
            Follower follower = getFollower(host);
            follower.close();
            followers.remove(follower);
        }

        lock.writeLock().unlock();
    }

    /**
     * Get the follower
     */
    public Follower getFollower(Host host) {
        lock.readLock().lock();

        try {
            return (Follower) followers.stream().filter(follower -> follower.getAddress().equals(host.getAddress()) && follower.getPort() == host.getPort());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Send the data to all the followers
     *
     * @return true if success to send the replica to all the followers else false if not able to send the replica to one or more followers.
     */
    public boolean send(byte[] data) {
        boolean isSuccess = true;
        lock.readLock().lock();

        //Sending the data to all the followers. Even when one of the follower failed.
        for (Follower follower : followers) {
            isSuccess = follower.send(data) && isSuccess;
        }

        lock.readLock().unlock();
        return isSuccess;
    }

    /**
     * Checks whether the given host exists in the collection
     */
    public boolean contains(Host follower) {
        lock.readLock().lock();

        try {
            return followers.stream().anyMatch(host -> host.getAddress().equals(follower.getAddress()) && host.getPort() == follower.getPort());
        } finally {
            lock.readLock().unlock();
        }
    }
}
