package org.tarantool;

import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Blocking console connection for test control purposes.
 *
 * Provides the means of lua commands evaluation given
 * the host and port or the instance name of tarantool.
 */
public abstract class TarantoolConsole implements Closeable {
    private final static Pattern GREETING_PATTERN = Pattern.compile("^Tarantool.+\n.+\n");
    private final static Pattern CONNECTED_PATTERN = Pattern.compile("^connected to (.*)\n");
    private final static Pattern REPLY_PATTERN = Pattern.compile("^.*\\n\\.{3}\\n",
        Pattern.UNIX_LINES | Pattern.DOTALL);

    private final static int TIMEOUT = 2000;
    private final StringBuilder unmatched = new StringBuilder();

    protected BufferedReader reader;
    protected OutputStreamWriter writer;

    private Matcher checkMatch(Pattern p) {
        if (unmatched.length() == 0)
            return null;

        Matcher m = p.matcher(unmatched.toString());

        if (m.find() && !m.requireEnd()) {
            unmatched.delete(0, m.end());
            return m;
        }

        return null;
    }

    protected Matcher expect(Pattern p) {
        Matcher result = checkMatch(p);
        if (result != null)
            return result;

        char[] buf = new char[4096];
        int rc;
        try {
            while ((rc = reader.read(buf, 0, buf.length)) > 0) {
                appendApplyBackspaces(unmatched, buf, rc);

                result = checkMatch(p);
                if (result != null)
                    return result;
            }
        } catch (SocketTimeoutException e) {
            throw new RuntimeException("Timeout occurred. Unmatched: " + unmatched.toString() + ", pattern:" + p, e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException("Unexpected end of input.");
    }

    private static void appendApplyBackspaces(StringBuilder sb, char[] buf, int len) {
        for (int i = 0; i < len ; i++) {
            char c = buf[i];
            if (c == '\b') {
                if (sb.length() > 0) {
                    sb.deleteCharAt(sb.length() - 1);
                }
            } else {
                sb.append(c);
            }
        }
    }

    protected void write(String expr) {
        try {
            writer.write(expr + '\n');
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            // No-op.
        }
        try {
            writer.close();
        } catch (IOException e) {
            // No-op.
        }
    }

    protected void suppressPrompt() {
        // No-op, override.
    }

    protected void suppressEcho(String expr) {
        // No-op, override.
    }

    public void exec(String expr) {
        suppressPrompt();
        write(expr);
        suppressEcho(expr);
        expect(REPLY_PATTERN);
    }

    public <T> T eval(String expr) {
        List<T> list = evalList(expr);
        return list.get(0);
    }

    public <T> List<T> evalList(String expr) {
        suppressPrompt();
        write(expr);
        suppressEcho(expr);
        Matcher m = expect(REPLY_PATTERN);
        Yaml yaml = new Yaml();
        return yaml.load(m.group(0));
    }

    /**
     * A direct tarantool console connection.
     *
     * @param host Tarantool host name.
     * @param port Console port of tarantool instance.
     * @return Console connection object.
     */
    public static TarantoolConsole open(String host, int port) {
        return new TarantoolTcpConsole(host, port);
    }

    /**
     * An indirect tarantool console connection via tarantoolctl utility.
     *
     * &gt; tarantoolctl enter &lt;instance&gt;
     *
     * This facility is aimed at support of multi-instance tests in future.
     *
     * @param workDir Directory where .tarantoolctl file is located.
     * @param instance Tarantool instance name as per  command.
     * @return Console connection object.
     */
    public static TarantoolConsole open(String workDir, String instance) {
        return new TarantoolLocalConsole(workDir, instance);
    }

    /**
     * A direct tarantool console connection (via TCP connection).
     */
    private static class TarantoolTcpConsole extends TarantoolConsole {
        final Socket socket;

        TarantoolTcpConsole(String host, int port) {
            socket = new TestSocketChannelProvider(host, port, TIMEOUT).get(1, null).socket();
            try {
                reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                writer = new OutputStreamWriter(socket.getOutputStream());
            } catch (IOException e) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                    // No-op.
                }
                throw new RuntimeException("Couldn't connect to console at " + host + ":" + port, e);
            }
            expect(GREETING_PATTERN);
        }

        @Override
        public void close() {
            super.close();

            try {
                socket.close();
            } catch (IOException e) {
                // No-op.
            }
        }
    }

    /**
     * An indirect tarantool console connection via tarantoolctl utility.
     */
    private static class TarantoolLocalConsole extends TarantoolConsole {
        final Process process;
        final String name;
        final Pattern prompt;

        TarantoolLocalConsole(String workDir, String instance) {
            ProcessBuilder builder = new ProcessBuilder("env", "tarantoolctl", "enter", instance);
            Map<String, String> env = builder.environment();
            env.put("PWD", workDir);
            env.put("TEST_WORKDIR", workDir);
            env.put("COLUMNS", "256");
            builder.redirectErrorStream(true);
            builder.directory(new File(workDir));

            try {
                process = builder.start();
            } catch (IOException e) {
                throw new RuntimeException("environment failure", e);
            }
            reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            writer = new OutputStreamWriter(process.getOutputStream());

            write("enter " + instance);
            Matcher m = expect(CONNECTED_PATTERN);
            name = m.group(1);
            prompt = Pattern.compile(Pattern.quote(name + "> "));
        }

        @Override
        protected void suppressPrompt() {
            expect(prompt);
        }

        @Override
        protected void suppressEcho(String expr) {
            expect(Pattern.compile(Pattern.quote(expr)));
        }

        @Override
        public void close() {
            super.close();
            process.destroy();
        }
    }
}
