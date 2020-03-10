package org.tarantool;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Describes an internal state of a registered request.
 */
public class TarantoolOperation implements Comparable<TarantoolOperation> {

    /**
     * A operation identifier.
     */
    private final long id;

    /**
     * Tarantool binary protocol operation code.
     */
    private final Code code;

    /**
     * Schema ID when this operation was registered.
     */
    private long sentSchemaId;

    /**
     * Schema ID when this operation was completed.
     */
    private long completedSchemaId;

    /**
     * Arguments of operation.
     */
    private final List<TarantoolRequestArgument> arguments;

    /**
     * Future request result.
     */
    private final CompletableFuture<?> result = new CompletableFuture<>();

    /**
     * Operation timeout.
     */
    private final Duration timeout;

    /**
     * Optional operation which is used for
     * schema synchronization purposes.
     */
    private TarantoolOperation dependedOperation;

    public TarantoolOperation(Code code,
                              List<TarantoolRequestArgument> arguments,
                              long id,
                              long schemaId,
                              Duration timeout) {
        this.id = id;
        this.sentSchemaId = schemaId;
        this.code = Objects.requireNonNull(code);
        this.arguments = new ArrayList<>(arguments);
        this.timeout =  timeout;
        setupTimeout(timeout);
    }

    public TarantoolOperation(Code code,
                              List<TarantoolRequestArgument> arguments,
                              long id,
                              long schemaId,
                              Duration timeout,
                              TarantoolOperation dependedOperation) {
        this.id = id;
        this.sentSchemaId = schemaId;
        this.code = Objects.requireNonNull(code);
        this.arguments = new ArrayList<>(arguments);
        this.timeout =  timeout;
        this.dependedOperation = dependedOperation;
        setupTimeout(timeout);
    }

    public long getId() {
        return id;
    }

    public long getSentSchemaId() {
        return sentSchemaId;
    }

    public void setSentSchemaId(long sentSchemaId) {
        this.sentSchemaId = sentSchemaId;
    }

    public long getCompletedSchemaId() {
        return completedSchemaId;
    }

    public void setCompletedSchemaId(long completedSchemaId) {
        this.completedSchemaId = completedSchemaId;
    }

    public CompletableFuture<?> getResult() {
        return result;
    }

    public Code getCode() {
        return code;
    }

    public boolean isSqlRelated() {
        return code == Code.EXECUTE || code == Code.PREPARE;
    }

    public TarantoolOperation getDependedOperation() {
        return dependedOperation;
    }

    public Duration getTimeout() {
        return timeout;
    }

    /**
     * Serializability means this requests is capable being
     * translated in a binary packet according to {@code iproto}
     * protocol.
     *
     * @return {@literal true} if this request is serializable
     */
    public boolean isSerializable() {
        return arguments.stream().allMatch(TarantoolRequestArgument::isSerializable);
    }

    public List<Object> getArguments() {
        return arguments.stream().map(TarantoolRequestArgument::getValue).collect(Collectors.toList());
    }

    @Override
    public int compareTo(TarantoolOperation other) {
        return Long.compareUnsigned(this.id, other.id);
    }

    private void setupTimeout(Duration duration) {
        if (duration == null) {
            return;
        }
        if (duration.isNegative()) {
            throw new IllegalArgumentException("Timeout cannot be negative");
        }
        if (duration.isZero() || result.isDone()) {
            return;
        }
        ScheduledFuture<?> abandonByTimeoutAction = TimeoutScheduler.EXECUTOR.schedule(
            () -> {
                if (!result.isDone()) {
                    result.completeExceptionally(new TimeoutException());
                }
            },
            duration.toMillis(), TimeUnit.MILLISECONDS
        );
        result.whenComplete((ignored, error) -> {
            if (error == null && !abandonByTimeoutAction.isDone()) {
                abandonByTimeoutAction.cancel(false);
            }
        });
    }

    /**
     * Runs timeout operation as a delayed task.
     */
    static class TimeoutScheduler {
        static final ScheduledThreadPoolExecutor EXECUTOR;

        static {
            EXECUTOR = new ScheduledThreadPoolExecutor(
                1, new TarantoolThreadDaemonFactory("tarantoolTimeout")
            );
            EXECUTOR.setRemoveOnCancelPolicy(true);
        }
    }

}
