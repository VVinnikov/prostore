package io.arenadata.dtm.jdbc.ext;

import io.arenadata.dtm.jdbc.core.BaseConnection;
import io.arenadata.dtm.jdbc.core.BaseStatement;
import io.arenadata.dtm.jdbc.core.ConnectionFactory;
import io.arenadata.dtm.jdbc.core.QueryExecutor;
import io.arenadata.dtm.jdbc.model.ColumnInfo;
import io.arenadata.dtm.jdbc.util.DtmSqlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class DtmConnectionImpl implements BaseConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(DtmConnectionImpl.class);
    public static final ZoneId DEFAULT_TIME_ZONE = ZoneId.of("UTC");
    /**
     * Hold level of resultSet
     */
    private int rsHoldability = ResultSet.CLOSE_CURSORS_AT_COMMIT;
    /**
     * Autocommit permission state on connection
     */
    private boolean autoCommit = true;
    private List<ColumnInfo> cachedFieldMetadata = new ArrayList<>();   //TODO need to update after changing table metadata
    /**
     * Executor for query
     */
    private final QueryExecutor queryExecutor;
    private final Properties clientInfo;
    // Connection's readonly state.
    private boolean readOnly = false;
    private SQLWarning firstWarning;

    public DtmConnectionImpl(String dbHost, String user, String schema, Properties info, String url) throws SQLException {
        this.queryExecutor = ConnectionFactory.openConnection(dbHost, user, schema, url, info);
        this.clientInfo = new Properties();
        LOGGER.info("Connection created host = {} schema = {} user = {}", dbHost, schema, user);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new DtmDatabaseMetaData(this);
    }

    @Override
    public BaseStatement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public BaseStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        return createStatement(resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        return prepareStatement(sql, resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return sql;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return this.autoCommit;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (this.autoCommit == autoCommit) {
            return;
        }
        this.autoCommit = autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void close() throws SQLException {
        this.getQueryExecutor().close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.getQueryExecutor().isClosed();
    }

    private void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new DtmSqlException("The connection was closed");
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return readOnly;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        this.readOnly = readOnly;
    }

    @Override
    public String getCatalog() throws SQLException {
        return this.getQueryExecutor().getDatabase();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        this.getQueryExecutor().setDatabase(catalog);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        SQLWarning newWarnings = queryExecutor.getWarnings();
        if (firstWarning == null) {
            firstWarning = newWarnings;
        } else if (newWarnings != null) {
            firstWarning.setNextWarning(newWarnings);
        }
        return firstWarning;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
        queryExecutor.getWarnings();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return rsHoldability;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public BaseStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        return new DtmStatement(this, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new DtmPreparedStatement(this, resultSetType, resultSetConcurrency, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return new DtmPreparedStatement(this, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw new DtmSqlException(String.format("Invalid timeout (%d<0).", timeout));
        }
        if (isClosed()) {
            return false;
        }
        try {
            PreparedStatement checkConnectionQuery = prepareStatement("CHECK_VERSIONS()");
            checkConnectionQuery.executeUpdate();
            return true;
        } catch (SQLException e) {
            LOGGER.error("Validating connection error.", e);
        }
        return false;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        clientInfo.put(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        clientInfo.putAll(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkClosed();
        return clientInfo.getProperty(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkClosed();
        return clientInfo;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getSchema() throws SQLException {
        return this.getQueryExecutor().getDatabase();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        this.getQueryExecutor().setDatabase(schema);
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        this.close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    @Override
    public String getUrl() {
        return this.queryExecutor.getUrl();
    }

    @Override
    public String getUserName() {
        return this.queryExecutor.getUser();
    }

    @Override
    public String getDBVersionNumber() {
        return this.queryExecutor.getServerVersion();
    }

    @Override
    public List<ColumnInfo> getCachedFieldMetadata() {
        return this.cachedFieldMetadata;
    }

    @Override
    public QueryExecutor getQueryExecutor() {
        return queryExecutor;
    }
}
