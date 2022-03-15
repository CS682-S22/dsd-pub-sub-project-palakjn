package utilities;

import configuration.Constants;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Responsible for maintaining a timer which starts, stop the timer and also let the caller know if timeout happen or not
 *
 * @author Palak Jain
 */
public class NodeTimer {
    Timer timer;
    boolean timeout;

    /**
     * Start the timer
     * @param packet the sequence number/packet type for which we have started the timer for
     */
    public void startTimer(String packet, long delay) {
        timer = new Timer();
        TimerTask task = new TimerTask() {
            public void run() {
                System.out.println("Time out for the packet: " + packet);
                timeout = true;
                timer.cancel();
                this.cancel();
            }
        };
        timer.schedule(task, delay);
    }

    /**
     * Stop the current running timer
     */
    public void stopTimer() {
        timer.cancel();
        timeout = false;
    }

    /**
     * @return true if timeout else false
     */
    public boolean isTimeout() {
        return timeout;
    }
}
