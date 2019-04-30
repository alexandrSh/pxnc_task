package pxnc.impl;

import pxnc.api.Scheduler;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

/**
 * QueueScheduler based on priority queue.
 *
 * @author Aleksandr_Sharomov
 */
public class QueueScheduler implements Scheduler {
    private static final AtomicLong SEQ = new AtomicLong(0);
    private final Queue<Task> queue = new PriorityQueue<>();
    private final TaskExecutor executor = new TaskExecutor(queue);

    public QueueScheduler() {
        executor.start();
    }

    @Override
    public <V> Future<V> apply(LocalDateTime time, Callable<V> callable) {

        FutureTask<V> fTask = new FutureTask<>(callable);
        Task<V> vTask = new Task<>(time, fTask);

        synchronized (queue) {
            queue.add(vTask);
            queue.notify();
        }

        return fTask;
    }

    private class Task<V> implements Comparable<Task<V>> {
        private final long seqNum;
        private final long time;
        private final FutureTask<V> futureTask;

        public Task(LocalDateTime localDateTime, FutureTask<V> futureTask) {
            this.time = localDateTime.atZone(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();

            this.futureTask = futureTask;
            this.seqNum = SEQ.getAndIncrement();
        }

        public long getTime() {
            return time;
        }

        public FutureTask<V> getFutureTask() {
            return futureTask;
        }

        @Override
        public int compareTo(Task<V> other) {
            int res = Long.compare(this.time, other.time);
            if (res == 0 && this.time == other.time)
                res = (this.seqNum < other.seqNum ? -1 : 1);
            return res;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Task<?> task = (Task<?>) o;
            return seqNum == task.seqNum &&
                    time == task.time &&
                    Objects.equals(futureTask, task.futureTask);
        }

        @Override
        public int hashCode() {
            return Objects.hash(seqNum, time, futureTask);
        }
    }

    private class TaskExecutor extends Thread {
        private final Queue<Task> queue;

        public TaskExecutor(Queue<Task> queue) {
            this.queue = queue;
        }


        @Override
        public void run() {
            try {
                loop();
            } catch (InterruptedException e) {
//                e.printStackTrace();
            }
        }

        private void loop() throws InterruptedException {
            while (true) {
                FutureTask<?> taskToRun = null;
                synchronized (queue) {
                    if (queue.isEmpty()) {
                        queue.wait();
                    }

                    if (queue.isEmpty()) {
                        continue;
                    }

                    Task<?> task = queue.peek();
                    long timeToSchedule = task.getTime();
                    long now = System.currentTimeMillis();

                    if (timeToSchedule > now) {
                        queue.wait(timeToSchedule - now);
                    }

                    Task<?> firstAfterWait = queue.peek();
                    if (firstAfterWait == task) {
                        if (firstAfterWait.getTime() <= System.currentTimeMillis()) {
                            taskToRun = firstAfterWait.getFutureTask();
                            queue.remove(firstAfterWait);
                        }
                    } else {
                        continue;
                    }
                }

                if (taskToRun != null) {
                    taskToRun.run();
                }
            }
        }

    }
}
