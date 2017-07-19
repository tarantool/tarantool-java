package org.tarantool;

public interface TarantoolSQLOps<Tuple, Update, Result> {
    Update update(String sql, Tuple... bind);

    Result query(String sql, Tuple... bind);
}
