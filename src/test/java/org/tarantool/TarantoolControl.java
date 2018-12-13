package org.tarantool;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
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
    public class TarantoolControlException extends RuntimeException {
        int code;
        String stdout;
        String stderr;

        TarantoolControlException(int code, String stdout, String stderr) {
            super("returned exitcode " + code + "\n" +
                "[stdout]\n" + stdout + "\n[stderr]\n" + stderr);
            this.code = code;
            this.stdout = stdout;
            this.stderr = stderr;
        }
    }

    protected static final String tntCtlWorkDir = System.getProperty("tntCtlWorkDir",
        new File("testroot").getAbsolutePath());
    protected static final String instanceDir = new File("src/test").getAbsolutePath();
    protected static final String tarantoolCtlConfig = new File("src/test/.tarantoolctl").getAbsolutePath();
    protected static final int RESTART_TIMEOUT = 2000;
    // Per-instance environment.
    protected final Map<String, Map<String, String>> instanceEnv = new HashMap<String, Map<String, String>>();

    static {
        try {
            setupWorkDirectory();
        } catch (IOException e) {
            throw new RuntimeException("Can't setup test root directory!", e);
        }
    }

    protected static void setupWorkDirectory() throws IOException {
        try {
            rmdir(tntCtlWorkDir);
        } catch (IOException ignored) {
            /* No-op. */
        }

        mkdir(tntCtlWorkDir);
        for (File c : new File(instanceDir).listFiles())
            if (c.getName().endsWith(".lua"))
                copyFile(c, tntCtlWorkDir);
        copyFile(tarantoolCtlConfig, tntCtlWorkDir);
    }

    // Based on https://stackoverflow.com/a/779529
    private static void rmdir(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                rmdir(c);
        }
        f.delete();
    }

    private static void rmdir(String f) throws IOException {
        rmdir(new File(f));
    }

    private static void mkdir(File f) throws IOException {
        f.mkdirs();
    }

    private static void mkdir(String f) throws IOException {
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
        env.putAll(buildInstanceEnvironment(instanceName));

        final Process process;
        try {
            process = builder.start();
        } catch (IOException e) {
            throw new RuntimeException("environment failure", e);
        }

        final CountDownLatch latch = new CountDownLatch(1);
        // The thread below is necessary to organize timed wait on the process.
        // We cannot use Process.waitFor(long, TimeUnit) because we're on java 6.
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
            throw new TarantoolControlException(code, stdout, stderr);
        }
    }

    /**
     * Wait until the instance will be started.
     *
     * Use tarantoolctl status instanceName.
     *
     * Then test the instance with TarantoolTcpConsole (ADMIN environment
     * variable is set) or TarantoolLocalConsole.
     */
    public void waitStarted(String instanceName) {
        while (status(instanceName) != 0)
            sleep();

        while (true) {
            try {
                openConsole(instanceName).close();
                break;
            } catch (Exception ignored) {
                /* No-op. */
            }
            sleep();
        }
    }

    /**
     * Wait until the instance will be stopped.
     *
     * Use tarantoolctl status instanceName.
     */
    public void waitStopped(String instanceName) {
        while (status(instanceName) != 1)
            sleep();
    }

    public void start(String instanceName) {
        executeCommand("start", instanceName);
    }

    public void stop(String instanceName) {
        executeCommand("stop", instanceName);
    }

    /**
     * Wrapper for `tarantoolctl status instanceName`.
     *
     * Return exit code of the command:
     *
     * * 0 -- started;
     * * 1 -- stopped;
     * * 2 -- pid file exists, control socket inaccessible.
     */
    public int status(String instanceName) {
        try {
            executeCommand("status", instanceName);
        } catch (TarantoolControlException e) {
            return e.code;
        }

        return 0;
    }

    public Map<String,String> buildInstanceEnvironment(String instanceName) {
        Map<String, String> env = new HashMap<String, String>();
        env.put("PWD", tntCtlWorkDir);
        env.put("TEST_WORKDIR", tntCtlWorkDir);

        Map<String, String> instanceEnv = this.instanceEnv.get(instanceName);
        if (instanceEnv != null) {
            env.putAll(instanceEnv);
        }
        return env;
    }

    public void createInstance(String instanceName, String luaFile, Map<String, String> env) {
        File src = new File(instanceDir, luaFile.endsWith(".lua") ? luaFile : luaFile.concat(".lua"));
        if (!src.exists())
            throw new RuntimeException("Lua file " + src + " doesn't exist.");

        File dst = new File(tntCtlWorkDir, instanceName + ".lua");
        try {
            copyFile(src, dst);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        instanceEnv.put(instanceName, env);
    }

    public void cleanupInstance(String instanceName) {
        instanceEnv.remove(instanceName);

        File dst = new File(tntCtlWorkDir, instanceName + ".lua");
        dst.delete();

        try {
            rmdir(new File(tntCtlWorkDir, instanceName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void waitReplication(String instanceName, int timeout) {
        TarantoolConsole console = openConsole(instanceName);
        try {
            TestUtils.waitReplication(console, timeout);
        } finally {
            console.close();
        }
    }

    /*
     * Open a console to the instance.
     *
     * Use text console (from ADMIN environment variable) when it is available
     * for the instance or fallback to TarantoolLocalConsole.
     */
    public TarantoolConsole openConsole(String instanceName) {
        Map<String, String> env = instanceEnv.get(instanceName);
        if (env == null)
            throw new RuntimeException("No such instance '" + instanceName +"'.");

        String admin = env.get("ADMIN");
        if (admin == null) {
            return TarantoolConsole.open(tntCtlWorkDir, instanceName);
        } else {
            int idx = admin.indexOf(':');
            return TarantoolConsole.open(idx < 0 ? "localhost" : admin.substring(0, idx),
                 Integer.valueOf(idx < 0 ? admin : admin.substring(idx + 1)));
        }
    }

    public static void sleep() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
