package org.tarantool;

import java.io.InputStream;

public abstract class CountInputStream extends InputStream {
    public abstract long getBytesRead();
}
