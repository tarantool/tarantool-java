package org.tarantool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by noviiden on 22/05/16.
 */
public class TarantoolConnection16Pool implements TarantoolConnection16{
    int poolSize = 16;
    Random random = new Random();
    List<TarantoolConnection16> pool = new ArrayList<TarantoolConnection16>();

    public TarantoolConnection16Pool(String host, int port, int poolSize) throws IOException {
        this.poolSize = poolSize;
        for(int i = 0; i < poolSize; i++){
            pool.add(new TarantoolConnection16Impl(host, port));
        }
    }

    public TarantoolConnection16 connection() {
        return pool.get(random.nextInt(1000000) % poolSize);
    }

    @Override
    public void auth(String username, String password) {
        for(TarantoolConnection16 con : pool){
            con.auth(username, password);
        }
    }

    @Override
    public void close() {
        for(TarantoolConnection16 con : pool){
            con.close();
        }
    }

    @Override
    public List select(Integer space, Integer index, Object key, int offset, int limit, int iterator) {
        return connection().select(space, index, key, offset, limit, iterator);
    }

    @Override
    public List insert(Integer space, Object tuple) {
        return connection().insert(space, tuple);
    }

    @Override
    public List replace(Integer space, Object tuple) {
        return connection().replace(space, tuple);
    }

    @Override
    public List update(Integer space, Object key, Object... tuple) {
        return connection().update(space, key, tuple);
    }

    @Override
    public List upsert(Integer space, Object key, Object defTuple, Object... ops) {
        return connection().upsert(space, key, defTuple, ops);
    }

    @Override
    public List delete(Integer space, Object key) {
        return connection().delete(space, key);
    }

    @Override
    public List call(String function, Object... args) {
        return connection().call(function, args);
    }

    @Override
    public List eval(String expression, Object... args) {
        return connection().eval(expression, args);
    }


    @Override
    public void ping() {

    }

}
