package io.pipelines.retry;

public interface RetryPolicy {
    boolean shouldRetry(int attempt, Exception e);
    long backoffMillis(int attempt);
}

