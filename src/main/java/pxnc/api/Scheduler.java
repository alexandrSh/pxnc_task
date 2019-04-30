package pxnc.api;

import java.time.LocalDateTime;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * Scheduler.
 *
 * @author Aleksandr_Sharomov
 */
public interface Scheduler {

    /**
     * Add Callable task to schedule.
     * Tasks performed in order according to the {@code time} value or in the order of the event arrival.
     *
     * @param time time to run.
     * @param callable task
     */
    <V> Future<V> apply(LocalDateTime time, Callable<V> callable);
}
