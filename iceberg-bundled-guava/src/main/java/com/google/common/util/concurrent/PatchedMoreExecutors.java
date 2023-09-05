package com.google.common.util.concurrent;

import com.google.common.annotations.VisibleForTesting;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Internal.toNanosSaturated;

/**
 * Copy all public methods of {@link MoreExecutors}.
 */
@SuppressWarnings("unused")
public final class PatchedMoreExecutors {
    private PatchedMoreExecutors() {
    }

    public static ExecutorService getExitingExecutorService(
        ThreadPoolExecutor executor, Duration terminationTimeout) {
        return getExitingExecutorService(
            executor, toNanosSaturated(terminationTimeout), TimeUnit.NANOSECONDS);
    }

    public static ExecutorService getExitingExecutorService(
        ThreadPoolExecutor executor, long terminationTimeout, TimeUnit timeUnit) {
        return new Application().getExitingExecutorService(executor, terminationTimeout, timeUnit);
    }

    public static ExecutorService getExitingExecutorService(ThreadPoolExecutor executor) {
        return new Application().getExitingExecutorService(executor);
    }

    public static ScheduledExecutorService getExitingScheduledExecutorService(
        ScheduledThreadPoolExecutor executor, Duration terminationTimeout) {
        return getExitingScheduledExecutorService(
            executor, toNanosSaturated(terminationTimeout), TimeUnit.NANOSECONDS);
    }

    public static ScheduledExecutorService getExitingScheduledExecutorService(
        ScheduledThreadPoolExecutor executor, long terminationTimeout, TimeUnit timeUnit) {
        return new Application()
            .getExitingScheduledExecutorService(executor, terminationTimeout, timeUnit);
    }

    public static ScheduledExecutorService getExitingScheduledExecutorService(
        ScheduledThreadPoolExecutor executor) {
        return new Application().getExitingScheduledExecutorService(executor);
    }

    public static void addDelayedShutdownHook(ExecutorService service, Duration terminationTimeout) {
        addDelayedShutdownHook(service, toNanosSaturated(terminationTimeout), TimeUnit.NANOSECONDS);
    }

    public static void addDelayedShutdownHook(
        ExecutorService service, long terminationTimeout, TimeUnit timeUnit) {
        new Application().addDelayedShutdownHook(service, terminationTimeout, timeUnit);
    }

    @VisibleForTesting
    static class Application {

        final ExecutorService getExitingExecutorService(
            ThreadPoolExecutor executor, long terminationTimeout, TimeUnit timeUnit) {
            useDaemonThreadFactory(executor);
            ExecutorService service = Executors.unconfigurableExecutorService(executor);
            addDelayedShutdownHook(executor, terminationTimeout, timeUnit);
            return service;
        }

        final ExecutorService getExitingExecutorService(ThreadPoolExecutor executor) {
            return getExitingExecutorService(executor, 120, TimeUnit.SECONDS);
        }

        final ScheduledExecutorService getExitingScheduledExecutorService(
            ScheduledThreadPoolExecutor executor, long terminationTimeout, TimeUnit timeUnit) {
            useDaemonThreadFactory(executor);
            ScheduledExecutorService service = Executors.unconfigurableScheduledExecutorService(executor);
            addDelayedShutdownHook(executor, terminationTimeout, timeUnit);
            return service;
        }

        final ScheduledExecutorService getExitingScheduledExecutorService(
            ScheduledThreadPoolExecutor executor) {
            return getExitingScheduledExecutorService(executor, 120, TimeUnit.SECONDS);
        }

        final void addDelayedShutdownHook(
            final ExecutorService service, final long terminationTimeout, final TimeUnit timeUnit) {
            checkNotNull(service);
            checkNotNull(timeUnit);
            addShutdownHook(
                MoreExecutors.newThread(
                    "DelayedShutdownHook-for-" + service,
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                // We'd like to log progress and failures that may arise in the
                                // following code, but unfortunately the behavior of logging
                                // is undefined in shutdown hooks.
                                // This is because the logging code installs a shutdown hook of its
                                // own. See Cleaner class inside {@link LogManager}.
                                service.shutdown();
                                service.awaitTermination(terminationTimeout, timeUnit);
                            } catch (InterruptedException ignored) {
                                // We're shutting down anyway, so just ignore.
                            }
                        }
                    }));
        }

        @VisibleForTesting
        void addShutdownHook(Thread hook) {
            Runtime.getRuntime().addShutdownHook(hook);
        }
    }

    private static void useDaemonThreadFactory(ThreadPoolExecutor executor) {
        executor.setThreadFactory(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setThreadFactory(executor.getThreadFactory())
                .build());
    }

    public static ListeningExecutorService newDirectExecutorService() {
        return MoreExecutors.newDirectExecutorService();
    }

    public static Executor directExecutor() {
        return MoreExecutors.directExecutor();
    }

    public static Executor newSequentialExecutor(Executor delegate) {
        return MoreExecutors.newSequentialExecutor(delegate);
    }

    public static ListeningExecutorService listeningDecorator(ExecutorService delegate) {
        return MoreExecutors.listeningDecorator(delegate);
    }

    public static ListeningScheduledExecutorService listeningDecorator(
        ScheduledExecutorService delegate) {
        return MoreExecutors.listeningDecorator(delegate);
    }

    public static ThreadFactory platformThreadFactory() {
        return MoreExecutors.platformThreadFactory();
    }

    public static boolean shutdownAndAwaitTermination(ExecutorService service, Duration timeout) {
        return shutdownAndAwaitTermination(service, toNanosSaturated(timeout), TimeUnit.NANOSECONDS);
    }

    public static boolean shutdownAndAwaitTermination(
        ExecutorService service, long timeout, TimeUnit unit) {
        long halfTimeoutNanos = unit.toNanos(timeout) / 2;
        // Disable new tasks from being submitted
        service.shutdown();
        try {
            // Wait for half the duration of the timeout for existing tasks to terminate
            if (!service.awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS)) {
                // Cancel currently executing tasks
                service.shutdownNow();
                // Wait the other half of the timeout for tasks to respond to being cancelled
                service.awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS);
            }
        } catch (InterruptedException ie) {
            // Preserve interrupt status
            Thread.currentThread().interrupt();
            // (Re-)Cancel if current thread also interrupted
            service.shutdownNow();
        }
        return service.isTerminated();
    }
}
