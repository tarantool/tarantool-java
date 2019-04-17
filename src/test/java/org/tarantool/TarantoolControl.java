package org.tarantool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper around tarantoolctl utility.
 */
public class TarantoolControl {

    public static class TarantoolControlException extends RuntimeException {

        int code;
        String stdout;
        String stderr;

        TarantoolControlException(int code, String stdout, String stderr) {
            super(
                "returned exitcode " + code + "\n" +
                    "[stdout]\n" + stdout +
                    "\n[stderr]\n" + stderr
            );
            this.code = code;
            this.stdout = stdout;
            this.stderr = stderr;
        }

    }

    protected static final String tntCtlWorkDir = System.getProperty("tntCtlWorkDir",
        new File("testroot").getAbsolutePath());
    protected static final String instanceDir = new File("src/test/resources").getAbsolutePath();
    protected static final String tarantoolCtlConfig = new File("src/test/resources/.tarantoolctl").getAbsolutePath();
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
        for (File c : new File(instanceDir).listFiles()) {
            if (c.getName().endsWith(".lua")) {
                copyFile(c, tntCtlWorkDir);
            }
        }
        copyFile(tarantoolCtlConfig, tntCtlWorkDir);
    }

    // Based on https://stackoverflow.com/a/779529
    private static void rmdir(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles()) {
                rmdir(c);
            }
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
        if (dest.isDirectory()) {
            dest = new File(dest, source.getName());
        }
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
        while ((line = br.readLine()) != null) {
            sb.append(line).append("\n");
        }
        return sb.toString();
    }

    /**
     * Executes a command of the given tarantool instance via
     * tarantoolctl utility.
     *
     * @param command      tarantoolctl utility command.
     * @param instanceName name of tarantool instance to control.
     */
    protected void executeControlCommand(String command, String instanceName) {
        ProcessCommand controlCommand =
            new ProcessCommand(buildInstanceEnvironment(instanceName), "env", "tarantoolctl", command, instanceName);
        if (!controlCommand.execute()) {
            throw new TarantoolControlException(
                controlCommand.getExitCode(),
                controlCommand.getOutput(),
                controlCommand.getError()
            );
        }
    }

    /**
     * Sends <code>start</code> command using tarantoolctl
     * and blocks until the process will be up.
     * <p>
     * Calling this method has the same effect as
     * {@code start(instanceName, true)}
     *
     * @param instanceName target instance name
     *
     * @see #start(String, boolean)
     */
    public void start(String instanceName) {
        start(instanceName, true);
    }

    /**
     * Sends <code>start</code> command using tarantoolctl
     * and optionally blocks until the process will be up.
     * <p>
     * The block includes real connection establishment using
     * {@link org.tarantool.TarantoolConsole}.
     *
     * @param instanceName target instance name
     * @see #waitStarted(String)
     */
    public void start(String instanceName, boolean wait) {
        executeControlCommand("start", instanceName);
        if (wait) {
            waitStarted(instanceName);
        }
    }

    /**
     * Waits until the instance will be started.
     * <p>
     * Use tarantoolctl status instanceName.
     * <p>
     * Then test the instance with TarantoolTcpConsole (ADMIN environment
     * variable is set) or TarantoolLocalConsole.
     */
    public void waitStarted(String instanceName) {
        while (status(instanceName) != 0) {
            sleep();
        }

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
     * Sends <code>stop</code> command using tarantoolctl
     * and blocks until the process will be down.
     *
     * @param instanceName target instance name
     */
    public void stop(String instanceName) {
        try {
            Path path = Paths.get(tntCtlWorkDir, instanceName + ".pid");
            if (Files.exists(path)) {
                String pid = new String(Files.readAllBytes(path));
                executeControlCommand("stop", instanceName);
                waitStopped(pid, instanceName);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Waits until the instance will be stopped.
     *
     * @param pid          instance PID
     * @param instanceName target instance name
     */
    private void waitStopped(String pid, String instanceName) {
        while (status(instanceName) != 1 || isProcessAlive(pid)) {
            sleep();
        }
    }

    private boolean isProcessAlive(String pid) {
        ProcessCommand sendSignal0Command =
            new ProcessCommand(Collections.emptyMap(), "kill", "-0", pid);
        return sendSignal0Command.execute();
    }

    /**
     * Wrapper for `tarantoolctl status instanceName`.
     * <p>
     * Return exit code of the command:
     * <p>
     * * 0 -- started;
     * * 1 -- stopped;
     * * 2 -- pid file exists, control socket inaccessible.
     */
    public int status(String instanceName) {
        try {
            executeControlCommand("status", instanceName);
        } catch (TarantoolControlException e) {
            return e.code;
        }

        return 0;
    }

    public Map<String, String> buildInstanceEnvironment(String instanceName) {
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
        if (!src.exists()) {
            throw new RuntimeException("Lua file " + src + " doesn't exist.");
        }

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
        if (env == null) {
            throw new RuntimeException("No such instance '" + instanceName + "'.");
        }

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

    static class ProcessCommand {

        final ProcessBuilder processBuilder;

        String lastOutput = "";
        String lastError = "";
        int lastExitCode = -1;

        public ProcessCommand(Map<String, String> environment, String... commands) {
            processBuilder = new ProcessBuilder(commands);
            processBuilder.directory(new File(tntCtlWorkDir));
            Map<String, String> env = processBuilder.environment();
            env.putAll(environment);
        }

        boolean execute() {
            final Process process;
            try {
                process = processBuilder.start();
            } catch (IOException e) {
                throw new RuntimeException("environment failure", e);
            }

            boolean res;
            try {
                res = process.waitFor(RESTART_TIMEOUT, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException("wait interrupted", e);
            }

            if (!res) {
                process.destroy();
                throw new RuntimeException("timeout");
            }

            lastExitCode = process.exitValue();
            try {
                lastOutput = loadStream(process.getInputStream()).trim();
                lastError = loadStream(process.getErrorStream()).trim();
            } catch (IOException ignored) {
                lastOutput = "";
                lastError = "";
            }

            return lastExitCode == 0;
        }

        String getOutput() {
            return lastOutput;
        }

        String getError() {
            return lastError;
        }

        int getExitCode() {
            return lastExitCode;
        }

    }

}

