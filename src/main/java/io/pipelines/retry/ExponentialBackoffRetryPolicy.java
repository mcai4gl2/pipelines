package io.pipelines.retry;

public class ExponentialBackoffRetryPolicy implements RetryPolicy {
    private final int maxAttempts;
    private final long baseMillis;
    private final long maxMillis;

    public ExponentialBackoffRetryPolicy(int maxAttempts, long baseMillis, long maxMillis) {
        this.maxAttempts = Math.max(1, maxAttempts);
        this.baseMillis = Math.max(1, baseMillis);
        this.maxMillis = Math.max(baseMillis, maxMillis);
    }

    @Override
    public boolean shouldRetry(int attempt, Exception e) {
        return attempt < maxAttempts;
    }

    @Override
    public long backoffMillis(int attempt) {
        long delay = baseMillis * (1L << Math.min(20, attempt - 1));
        return Math.min(delay, maxMillis);
    }
}

