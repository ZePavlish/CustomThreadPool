package com.example.threadpool;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

public class CustomThreadPool implements CustomExecutor {
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int queueSize;
    private final int minSpareThreads;

    private final AtomicInteger threadCount = new AtomicInteger(0);
    private final AtomicInteger activeThreads = new AtomicInteger(0);
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    private final List<BlockingQueue<Runnable>> queues;
    private final List<Worker> workers;
    private final ThreadFactory threadFactory;
    private final CustomRejectedExecutionHandler rejectionHandler;

    private final Lock mainLock = new ReentrantLock();
    private final Condition termination = mainLock.newCondition();

    public CustomThreadPool(int corePoolSize, int maxPoolSize, long keepAliveTime,
                            TimeUnit timeUnit, int queueSize, int minSpareThreads,
                            ThreadFactory threadFactory, CustomRejectedExecutionHandler rejectionHandler) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;
        this.threadFactory = threadFactory != null ? threadFactory : new DefaultThreadFactory();
        this.rejectionHandler = rejectionHandler != null ? rejectionHandler : new DefaultRejectionHandler();

        this.queues = new ArrayList<>(maxPoolSize);
        this.workers = new ArrayList<>(maxPoolSize);

        // Инициализация основных потоков
        for (int i = 0; i < corePoolSize; i++) {
            addWorker();
        }
    }

    @Override
    public void execute(Runnable command) {
        if (isShutdown.get()) {
            rejectionHandler.rejectedExecution(command, this);
            return;
        }

        Worker worker = findAvailableWorker();
        if (worker != null) {
            worker.submit(command);
            return;
        }

        if (threadCount.get() < maxPoolSize) {
            if (addWorker()) {
                workers.get(workers.size() - 1).submit(command);
                return;
            }
        }

        rejectionHandler.rejectedExecution(command, this);
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        FutureTask<T> future = new FutureTask<>(callable);
        execute(future);
        return future;
    }

    @Override
    public void shutdown() {
        isShutdown.set(true);
        workers.forEach(Worker::interruptIfIdle);
    }

    @Override
    public void shutdownNow() {
        isShutdown.set(true);
        workers.forEach(Worker::interrupt);
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        mainLock.lock();
        try {
            while (threadCount.get() > 0) {
                if (nanos <= 0L) {
                    return false;
                }
                nanos = termination.awaitNanos(nanos);
            }
            return true;
        } finally {
            mainLock.unlock();
        }
    }

    private Worker findAvailableWorker() {
        int size = workers.size();
        if (size == 0) return null;

        int start = ThreadLocalRandom.current().nextInt(size);
        for (int i = 0; i < size; i++) {
            int index = (start + i) % size;
            Worker worker = workers.get(index);
            if (worker.isAvailable()) {
                return worker;
            }
        }

        int available = size - activeThreads.get();
        if (available < minSpareThreads && threadCount.get() < maxPoolSize) {
            if (addWorker()) {
                return workers.get(workers.size() - 1);
            }
        }

        return null;
    }

    private boolean addWorker() {
        if (threadCount.get() >= maxPoolSize) {
            return false;
        }

        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(queueSize);
        Worker worker = new Worker(queue);
        Thread thread = threadFactory.newThread(worker);

        if (thread != null) {
            worker.bindThread(thread);
            workers.add(worker);
            threadCount.incrementAndGet();
            thread.start();
            return true;
        }

        return false;
    }

    private void workerTerminated(Worker worker) {
        mainLock.lock();
        try {
            workers.remove(worker);
            threadCount.decrementAndGet();
            if (threadCount.get() == 0) {
                termination.signalAll();
            }
        } finally {
            mainLock.unlock();
        }
    }

    private class Worker implements Runnable {
        private final BlockingQueue<Runnable> queue;
        private Thread thread;
        private volatile boolean running = true;
        private final AtomicBoolean working = new AtomicBoolean(false);

        public Worker(BlockingQueue<Runnable> queue) {
            this.queue = queue;
            queues.add(queue);
        }

        public void bindThread(Thread thread) {
            this.thread = thread;
        }

        public void submit(Runnable task) {
            if (queue.offer(task)) {
                System.out.printf("[Pool] Task accepted into queue #%d: %s%n",
                        workers.indexOf(this), task);
            } else {
                rejectionHandler.rejectedExecution(task, CustomThreadPool.this);
            }
        }

        public boolean isAvailable() {
            return queue.remainingCapacity() > 0 && !working.get();
        }

        public void interruptIfIdle() {
            if (working.compareAndSet(false, true)) {
                thread.interrupt();
            }
        }

        public void interrupt() {
            thread.interrupt();
        }

        @Override
        public void run() {
            try {
                while (running && !Thread.currentThread().isInterrupted()) {
                    Runnable task = null;
                    working.set(false);

                    try {
                        task = queue.poll(keepAliveTime, timeUnit);
                    } catch (InterruptedException e) {
                        if (!running) {
                            break;
                        }
                    }

                    if (task != null) {
                        working.set(true);
                        activeThreads.incrementAndGet();

                        try {
                            System.out.printf("[Worker] %s executes %s%n",
                                    Thread.currentThread().getName(), task);
                            task.run();
                        } catch (Exception e) {
                            System.err.printf("Task %s failed: %s%n", task, e.getMessage());
                        } finally {
                            activeThreads.decrementAndGet();
                        }
                    } else if (threadCount.get() > corePoolSize) {
                        System.out.printf("[Worker] %s idle timeout, stopping.%n",
                                Thread.currentThread().getName());
                        break;
                    }
                }
            } finally {
                running = false;
                System.out.printf("[Worker] %s terminated.%n", Thread.currentThread().getName());
                workerTerminated(this);
            }
        }
    }

    private static class DefaultThreadFactory implements ThreadFactory {
        private final AtomicInteger counter = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, "CustomPool-worker-" + counter.getAndIncrement());
            System.out.printf("[ThreadFactory] Creating new thread: %s%n", thread.getName());
            return thread;
        }
    }

    private static class DefaultRejectionHandler implements CustomRejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, CustomThreadPool executor) {
            System.out.printf("[Rejected] Task %s was rejected due to overload!%n", r);
            throw new RuntimeException("Task " + r + " rejected from " + executor);
        }
    }

    public static class CallerRunsPolicy implements CustomRejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, CustomThreadPool executor) {
            if (!executor.isShutdown.get()) {
                System.out.printf("[Rejected] Task %s will be executed in caller thread%n", r);
                r.run();
            }
        }
    }

    public static class DiscardPolicy implements CustomRejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, CustomThreadPool executor) {
            System.out.printf("[Rejected] Task %s was silently discarded%n", r);
        }
    }

    public interface CustomRejectedExecutionHandler {
        void rejectedExecution(Runnable r, CustomThreadPool executor);
    }
}

interface CustomExecutor extends Executor {
    void execute(Runnable command);
    <T> Future<T> submit(Callable<T> callable);
    void shutdown();
    void shutdownNow();
}