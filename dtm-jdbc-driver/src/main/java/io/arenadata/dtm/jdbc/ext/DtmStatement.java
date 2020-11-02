package io.arenadata.dtm.jdbc.ext;

import io.arenadata.dtm.jdbc.core.Field;
import io.arenadata.dtm.jdbc.core.QueryResult;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

@Slf4j
public class DtmStatement implements Statement {

    /**
     * Текущее подключение
     */
    protected DtmConnection connection;
    /**
     * Тип возвращаемого resultSet'a (ResultSet.TYPE_xxx)
     */
    protected final int resultSetScrollType;
    /**
     * Является ли обновляемым (ResultSet.CONCUR_xxx)
     */
    protected final int concurrency;

    /**
     * Максимальное количество строк в ответе
     */
    protected long maxRows;

    /**
     * Количество строк в блоке
     */
    protected int fetchSize;

    /**
     * Текущий ResultSet
     */
    protected DtmResultSet resultSet;

    public DtmStatement(DtmConnection c, int rsType, int rsConcurrency) {
        this.connection = c;
        resultSetScrollType = rsType;
        concurrency = rsConcurrency;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (executeInternal(sql, fetchSize, Statement.NO_GENERATED_KEYS)) {
            return resultSet;
        }
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        log.debug("execute: {}", sql);
        return executeInternal(sql, fetchSize, Statement.NO_GENERATED_KEYS);
    }

    private boolean executeInternal(String sql, int fetchSize, int noGeneratedKeys) throws SQLException {
        log.debug("executeInternal: {}", sql);
        QueryResult queryResult = connection.protocol.executeQuery(sql);
        if (queryResult.getResult() != null) {
            List<Field[]> result = new ArrayList<>();
            List<Map<String, Object>> rows = queryResult.getResult();

            rows.forEach(row -> {
                Field[] resultFields = new Field[row.size()];
                IntStream.range(0, queryResult.getMetadata().size()).forEach(key -> {
                    String columnName = queryResult.getMetadata().get(key).getName();
                    resultFields[key] = new Field(columnName, row.get(columnName));
                });
                result.add(resultFields);
            });
            resultSet = new DtmResultSet(connection,
                    result,
                    queryResult.getMetadata(),
                    Collections.emptyList(),
                    ZoneId.of(queryResult.getTimeZone()));
        }
        return resultSet != null;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        log.debug("executeUpdate: {}", sql);
        execute(sql);
        return 1;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return (int) maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {

    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return -1;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return this.fetchSize;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (rows < 0 && rows != Integer.MIN_VALUE) {
            throw new SQLException(String.format("Incorrect %d value for block size", rows));
        } else if (rows == Integer.MIN_VALUE) {
            //for compatibility Integer.MIN_VALUE is transform to 0 => streaming
            this.fetchSize = 1;
            return;
        }
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return resultSetScrollType;
    }

    @Override
    public void addBatch(String sql) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return executeInternal(sql, fetchSize, autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return executeInternal(sql, fetchSize, Statement.RETURN_GENERATED_KEYS);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return executeInternal(sql, fetchSize, Statement.RETURN_GENERATED_KEYS);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        close();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
