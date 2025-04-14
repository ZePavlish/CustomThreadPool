package com.example.threadpool;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class Main {
    public static void main(String[] args) throws Exception {
        CustomThreadPool pool = new CustomThreadPool(
                2, // corePoolSize
                4, // maxPoolSize
                5, // keepAliveTime
                TimeUnit.SECONDS, // timeUnit
                5, // queueSize
                1, // minSpareThreads
                null, // threadFactory (по умолчанию)
                new CustomThreadPool.CallerRunsPolicy() // rejectionHandler
        );

        AtomicInteger taskCounter = new AtomicInteger(1);

        for (int i = 0; i < 15; i++) {
            final int taskId = taskCounter.getAndIncrement();
            try {
                pool.execute(() -> {
                    System.out.printf("Task %d started in %s%n",
                            taskId, Thread.currentThread().getName());
                    try {
                        Thread.sleep(1000 + ThreadLocalRandom.current().nextInt(2000));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    System.out.printf("Task %d completed in %s%n",
                            taskId, Thread.currentThread().getName());
                });
            } catch (Exception e) {
                System.err.printf("Error submitting task %d: %s%n", taskId, e.getMessage());
            }

            if (i % 3 == 0) {
                Thread.sleep(500);
            }
        }

        Thread.sleep(8000);

        System.out.println("Shutting down pool...");
        pool.shutdown();

        if (pool.awaitTermination(10, TimeUnit.SECONDS)) {
            System.out.println("All tasks completed successfully");
        } else {
            System.out.println("Some tasks were not completed");
        }
    }
}