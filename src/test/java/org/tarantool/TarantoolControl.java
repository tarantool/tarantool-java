package org.tarantool;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Wrapper around tarantoolctl utility.
 */
public class TarantoolControl {
    protected static final String tntCtlWorkDir = System.getProperty("tntCtlWorkDir",
        new File("testroot").getAbsolutePath());
    protected static final String instanceDir = new File("src/test").getAbsolutePath();
    protected static final String tarantoolCtlConfig = new File("src/test/.tarantoolctl").getAbsolutePath();
    protected static final int RESTART_TIMEOUT = 2000;

    // Based on https://stackoverflow.com/a/779529
    private void rmdir(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                rmdir(c);
        }
        f.delete();
    }

    private void rmdir(String f) throws IOException {
        rmdir(new File(f));
    }

    private void mkdir(File f) throws IOException {
        f.mkdirs();
    }

    private void mkdir(String f) throws IOException {
        mkdir(new File(f));
    }

    private static void copyFile(File source, File dest) throws IOException {
        if (dest.isDirectory())
            dest = new File(dest, source.getName());
        FileChannel sourceChannel = null;
        FileChannel destChannel = null;
        try {
            sourceChannel = new FileInputStream(source).getChannel();
            destChannel = new FileOutputStream(dest).getChannel();
            destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());
        } finally {
            sourceChannel.close();
            destChannel.close();
        }
    }

    private static void copyFile(String source, String dest) throws IOException {
        copyFile(new File(source), new File(dest));
    }

    private static void copyFile(File source, String dest) throws IOException {
        copyFile(source, new File(dest));
    }

    private static void copyFile(String source, File dest) throws IOException {
        copyFile(new File(source), dest);
    }

    private static String loadStream(InputStream s) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(s));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null)
            sb.append(line).append("\n");
        return sb.toString();
    }

    protected void setupWorkDirectory() throws IOException {
        rmdir(tntCtlWorkDir);
        mkdir(tntCtlWorkDir);
        for (File c : new File(instanceDir).listFiles())
            if (c.getName().endsWith(".lua"))
                copyFile(c, tntCtlWorkDir);
        copyFile(tarantoolCtlConfig, tntCtlWorkDir);
    }

    TarantoolControl() {
        try {
            setupWorkDirectory();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Control the given tarantool instance via tarantoolctl utility.
     *
     * @param command A tarantoolctl utility command.
     * @param instanceName Name of tarantool instance to control.
     */
    protected void executeCommand(String command, String instanceName) {
        ProcessBuilder builder = new ProcessBuilder("env", "tarantoolctl", command, instanceName);
        builder.directory(new File(tntCtlWorkDir));
        Map<String, String> env = builder.environment();
        env.put("PWD", tntCtlWorkDir);
        env.put("TEST_WORKDIR", tntCtlWorkDir);

        final Process process;
        try {
            process = builder.start();
        } catch (IOException e) {
            throw new RuntimeException("environment failure", e);
        }

        final CountDownLatch latch = new CountDownLatch(1);
        // The thread below is necessary to organize timed wait on the process.
        // We cannot use Process.waitFor(long, TimeUnit) because we on java 6.
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    process.waitFor();
                } catch (InterruptedException e) {
                    // No-op.
                }
                latch.countDown();
            }
        });

        thread.start();

        boolean res;
        try {
            res = latch.await(RESTART_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException("wait interrupted", e);
        }

        if (!res) {
            thread.interrupt();
            process.destroy();

            throw new RuntimeException("timeout");
        }

        int code = process.exitValue();

        if (code != 0) {
            String stdout = "";
            String stderr = "";
            try {
                stdout = loadStream(process.getInputStream());
                stderr = loadStream(process.getErrorStream());
            } catch (IOException e) {
                /* No-op. */
            }
            throw new RuntimeException("returned exitcode " + code + "\n" +
                "[stdout]\n" + stdout + "\n[stderr]\n" + stderr);
        }
    }

    public void start(String instanceName) {
        executeCommand("start", instanceName);
    }

    public void stop(String instanceName) {
        executeCommand("stop", instanceName);
    }
}
