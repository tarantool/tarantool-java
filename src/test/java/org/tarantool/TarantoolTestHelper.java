package org.tarantool;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class TarantoolTestHelper {

    public static final String HOST = System.getProperty("tntHost", "localhost");
    public static final int PORT = Integer.parseInt(System.getProperty("tntPort", "3301"));
    public static final int ADMIN_PORT = Integer.parseInt(System.getProperty("tntConsolePort", "3313"));
    public static final String USERNAME = System.getProperty("tntUser", "test_admin");
    public static final String PASSWORD = System.getProperty("tntPass", "4pWBZmLEgkmKK5WP");
    public static final String LUA_FILE = System.getProperty("tntBootstrapFile", "jdk-testing.lua");

    private TarantoolControl control;
    private TarantoolConsole console;

    private final String instanceName;

    public TarantoolTestHelper(String instanceName) {
        control = new TarantoolControl();
        this.instanceName = instanceName;
    }

    public void startInstance() {
        control.start(instanceName);
    }

    public void startInstanceAsync() {
        control.start(instanceName, false);
    }

    public void awaitStart() {
        control.waitStarted(instanceName);
    }

    public void awaitReplication(Duration duration) {
        control.waitReplication(instanceName, (int) duration.toMillis());
    }

    public void createInstance() {
        createInstance(LUA_FILE, PORT, ADMIN_PORT);
    }

    public void createInstance(String luaFile, int listenPort, int consolePort) {
        control.createInstance(instanceName, luaFile, makeInstanceEnv(listenPort, consolePort));
    }

    public void createInstance(String luaFile,
                               int listenPort,
                               int consolePort,
                               String replicationConfig,
                               double replicationTimeout) {
        control.createInstance(
            instanceName,
            luaFile,
            makeInstanceEnv(listenPort, consolePort, replicationConfig, replicationTimeout)
        );
    }

    public void stopInstance() {
        destroyConsole();
        control.stop(instanceName);
    }

    public String getInstanceVersion() {
        return evaluate("return box.info.version");
    }

    public <T> T evaluate(String expression) {
        initConsole();
        return console.eval(expression);
    }

    public void executeSql(String... sql) {
        initConsole();
        for (String statement : sql) {
            console.exec("box.execute(\"" + statement + "\")");
        }
    }

    public void executeLua(String... lua) {
        initConsole();
        for (String statement : lua) {
            console.exec(statement);
        }
    }

    private Map<String, String> makeInstanceEnv(int port, int consolePort) {
        Map<String, String> env = new HashMap<String, String>();
        env.put("LISTEN", Integer.toString(port));
        env.put("ADMIN", Integer.toString(consolePort));
        return env;
    }

    private Map<String, String> makeInstanceEnv(int port,
                                                int consolePort,
                                                String replicationConfig,
                                                double replicationTimeout) {
        Map<String, String> env = makeInstanceEnv(port, consolePort);
        env.put("MASTER", replicationConfig);
        env.put("REPLICATION_TIMEOUT", Double.toString(replicationTimeout));
        return env;
    }

    private void initConsole() {
        if (console == null) {
            console = TarantoolConsole.open(TarantoolControl.tntCtlWorkDir, instanceName);
        }
    }

    private void destroyConsole() {
        if (console != null) {
            console.close();
            console = null;
        }
    }

}
