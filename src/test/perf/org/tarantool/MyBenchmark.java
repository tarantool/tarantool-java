package org.tarantool;

import org.openjdk.jmh.annotations.*;
import org.tarantool.server.TarantoolInstanceConnection;

import java.io.IOException;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;

public class MyBenchmark {

    private static class DodgeSocketChannelProvider implements SocketChannelProvider {

        private final Integer defaultSocketQueueSize;

        private DodgeSocketChannelProvider(Integer defaultSocketQueueSize) {
            this.defaultSocketQueueSize = defaultSocketQueueSize;
        }

        @Override
        public SocketChannel get() {
            return new DodgeSocketChannel(defaultSocketQueueSize);
        }
    }

    /**
     * todo
     */
    private static class DodgeCommunication implements InstanceConnectionProvider {
        @Override
        public TarantoolInstanceConnection connect() throws IOException {
            return null;
        }

        @Override
        public String getDescription() {
            return null;
        }
    }


    @State(Scope.Benchmark)
    public static class SpeedOfWriteAndReadState {

        public TarantoolClientImpl tarantoolClient;

        @Setup(Level.Invocation)
        public void init() {
            System.out.println("INIT BENCHMARK");
            TarantoolClientConfig config = new TarantoolClientConfig();
            this.tarantoolClient = new TarantoolClientImpl(new DodgeCommunication(), config);
        }

        @TearDown(Level.Invocation)
        public void tearDown() {
            System.out.println("TEARDOWN BENCHMARK");
            tarantoolClient.close();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MINUTES)
    public void measureSpeedOfWriteAndReadViaSharedBuffer(SpeedOfWriteAndReadState state) {
        try {
            System.out.println("RUN BENCHMARK. state is " + state);
            state.tarantoolClient.asyncOps().insert(1, Arrays.asList("a", "b")).get();
        } catch (Exception e) {
            throw new IllegalArgumentException("Exception occurred while benchmarking", e);
        }
    }

    @State(Scope.Thread)
    public static class MyState {
        public int a = 1;
        public int b = 2;
        public int sum ;
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MINUTES)
    public void testMethod(MyState state) {
        state.sum = state.a + state.b;
        System.out.println("Run from: " + Thread.currentThread().getName());
    }

}
