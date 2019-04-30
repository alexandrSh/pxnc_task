package pxnc.impl;

import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * QueueSchedulerTest.
 *
 * @author Aleksandr_Sharomov
 */
public class QueueSchedulerTest {

    private Function<Long, TestData> operation = timeToSleep -> {
        TestData testData = new TestData();
        testData.start();
        try {
            Thread.sleep(timeToSleep);
        } catch (InterruptedException e) {
            //
        }
        testData.end();
        return testData;
    };


    @Test
    public void testAddTaskWithEarlyStart() throws ExecutionException, InterruptedException {
        QueueScheduler scheduler = new QueueScheduler();

        LocalDateTime firstLocalDateTime = LocalDateTime.now().plusSeconds(3);
        Future<TestData> first = scheduler.apply(firstLocalDateTime, () -> operation.apply(500L));

        LocalDateTime secondLocalDateTime = LocalDateTime.now().plusSeconds(1);
        Future<TestData> second = scheduler.apply(secondLocalDateTime, () -> operation.apply(500L));

        Assert.assertTrue(first.get().getStart().isAfter(second.get().getStart()));
        Assert.assertTrue(first.get().getStart().isAfter(second.get().getEnd()));

        LocalDateTime fStart = first.get().getStart().truncatedTo(ChronoUnit.SECONDS);
        Assert.assertEquals(firstLocalDateTime.truncatedTo(ChronoUnit.SECONDS), fStart);

        LocalDateTime sStart = second.get().getStart().truncatedTo(ChronoUnit.SECONDS);
        Assert.assertEquals(secondLocalDateTime.truncatedTo(ChronoUnit.SECONDS), sStart);

    }

    @Test
    public void testTaskWithLongOperation() throws ExecutionException, InterruptedException {
        QueueScheduler scheduler = new QueueScheduler();

        LocalDateTime taskTime = LocalDateTime.now().plusSeconds(1);

        Future<TestData> first = scheduler.apply(taskTime, () -> operation.apply(1000L));


        Future<TestData> second = scheduler.apply(taskTime, () -> operation.apply(10L));

        Assert.assertTrue(first.get().getStart().isBefore(second.get().getStart()));
        Assert.assertTrue(first.get().getEnd().isBefore(second.get().getEnd()));

        LocalDateTime fStart = first.get().getStart().truncatedTo(ChronoUnit.SECONDS);
        Assert.assertEquals(taskTime.truncatedTo(ChronoUnit.SECONDS), fStart);

        LocalDateTime sStart = second.get().getStart().truncatedTo(ChronoUnit.SECONDS);
        Assert.assertEquals(taskTime.plusSeconds(1).truncatedTo(ChronoUnit.SECONDS), sStart);
    }

    @Test
    public void testMultiThreading() {
        QueueScheduler scheduler = new QueueScheduler();

        LocalDateTime taskTime = LocalDateTime.now().plusSeconds(1);

        ExecutorService executorService = Executors.newFixedThreadPool(3);

        List<TestData> testDatas = IntStream.range(0, 10)
                .boxed()
                .map(i -> executorService.submit(() -> scheduler.apply(taskTime, () -> operation.apply(100L))))
                .map(f -> {
                    try {
                        return f.get();
                    } catch (Exception e) {
                        throw new RuntimeException();
                    }
                })
                .map(f -> {
                    try {
                        return f.get();
                    } catch (Exception e) {
                        throw new RuntimeException();
                    }
                })
                .collect(Collectors.toList());


        for (int j = 1; j < testDatas.size(); j++) {
            TestData t1 = testDatas.get(j - 1);
            TestData t2 = testDatas.get(j);

            Assert.assertTrue(t1.getStart().isBefore(t2.getStart()));
            Assert.assertTrue(t1.getEnd().isBefore(t2.getStart()) || t1.getEnd().isEqual(t2.getStart()));
        }
    }


    class TestData {
        private LocalDateTime start;
        private LocalDateTime end;

        public LocalDateTime getStart() {
            return start;
        }

        public void start() {
            this.start = LocalDateTime.now();
        }

        public LocalDateTime getEnd() {
            return end;
        }

        public void end() {
            this.end = LocalDateTime.now();
        }


        @Override
        public String toString() {
            return "TestData{" +
                    "start=" + start +
                    ", end=" + end +
                    '}';
        }
    }
}