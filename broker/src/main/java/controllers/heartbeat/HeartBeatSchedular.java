package controllers.heartbeat;

import configurations.BrokerConstants;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Responsible for scheduling tasks at a fixed interval to send heart beat messages.
 *
 * @author Palak Jain
 */
public class HeartBeatSchedular {
    private static ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(BrokerConstants.MAX_HEARTBEAT_TASKS);
    private static Map<String, ScheduledFuture<?>> scheduledTasks = new HashMap<>();

    private HeartBeatSchedular() {}

    /**
     * Schedule the given task to run at the fixed periodic interval
     */
    public synchronized static void start(String taskName, Runnable task, long period) {
        ScheduledFuture<?> scheduledTask = executor.scheduleWithFixedDelay(task, period, period, TimeUnit.MILLISECONDS);
        scheduledTasks.put(taskName, scheduledTask);
    }

    /**
     * Cancel the task
     */
    public synchronized static void cancel(String taskName) {
        ScheduledFuture<?> task = scheduledTasks.get(taskName);
        task.cancel(true);

        scheduledTasks.remove(taskName);
    }
}
