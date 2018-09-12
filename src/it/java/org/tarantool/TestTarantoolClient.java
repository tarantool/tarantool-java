package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;


public class TestTarantoolClient {

    public static final int TESTER_SPACE_ID = 513;

    /*
              Before executing this test you should configure your local tarantool

              box.cfg{listen=3301}
              space = box.schema.space.create('tester')
              box.space.tester:create_index('primary', {type = 'hash', parts = {1, 'NUM'}})

              box.schema.user.create('test', { password = 'test' })
              box.schema.user.grant('test', 'execute,received,write', 'universe')
              box.space.tester:format{{name='id',type='num'},{name='text',type='str'}}
             */
    public static class TarantoolClientTestImpl extends TarantoolClientImpl {
        final Semaphore s = new Semaphore(0);
        long latency = 1L;

        public TarantoolClientTestImpl(SocketChannelProvider socketProvider, TarantoolClientConfig options) {
            super(socketProvider, options);
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    try {
                        System.out.println("manually closed");
                        channel.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            t.setDaemon(true);
            t.start();
        }

        @Override
        protected void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
            try {
                Thread.sleep(1L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            super.writeFully(channel, buffer);
        }

        @Override
        protected void configureThreads(String threadName) {
            super.configureThreads(threadName);
            reader.setDaemon(true);
            writer.setDaemon(true);
        }

        @Override
        protected void reconnect(int retry, Throwable lastError) {
            if (s != null) {
                s.release(wait.get());
            }
            super.reconnect(retry, lastError);
        }

        @Override
        protected void complete(long code, FutureImpl<?> q) {
            super.complete(code, q);
            if (code != 0) {
                System.out.println(code);
            }
            s.release();
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException, SQLException {
        final int calls = 1000;

        TarantoolClientConfig config = new TarantoolClientConfig();
        config.username = "test";
        config.password = "test";
        config.initTimeoutMillis = 1000;

        config.sharedBufferSize = 128;

        //config.sharedBufferSize = 0;
        SocketChannelProvider socketChannelProvider = new SocketChannelProvider() {
            @Override
            public SocketChannel get(int retryNumber, Throwable lastError) {
                if (lastError != null) {
                    lastError.printStackTrace(System.out);
                }
                System.out.println("reconnect");
                try {
                    return SocketChannel.open(new InetSocketAddress("localhost", 3301));
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        };
        final TarantoolClientTestImpl client = new TarantoolClientTestImpl(socketChannelProvider, config);
        config.writeTimeoutMillis = 2;
        client.latency = 1;
        client.syncOps.ping();
        long st = System.currentTimeMillis();
        final int threads = 16;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            exec.execute(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < Math.ceil((double) calls / threads); i++) {
                        try {
                            client.fireAndForgetOps().replace(TESTER_SPACE_ID, Arrays.asList(ByteBuffer.allocate(4).putInt(i % 10000).array(), new byte[]{0,1,2,3,4,5,6}));
                        } catch (Exception e) {
                            try {
                                client.waitAlive();
                            } catch (InterruptedException e1) {
                                e1.printStackTrace();
                            }
                            i--;
                        }

                    }
                }
            });
        }
        exec.shutdown();
        exec.awaitTermination(1, TimeUnit.HOURS);
        System.out.println("pushed " + (System.currentTimeMillis() - st) + "ms \n" + client.stats.toString());
        client.s.acquire(calls);
        client.close();
        System.out.println("completed " + (System.currentTimeMillis() - st) + "ms \n" + client.stats.toString());

    }
}
