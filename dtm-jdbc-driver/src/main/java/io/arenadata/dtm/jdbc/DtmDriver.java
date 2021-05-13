package io.arenadata.dtm.jdbc;

import io.arenadata.dtm.jdbc.ext.DtmConnectionImpl;
import io.arenadata.dtm.jdbc.util.DriverInfo;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

import static io.arenadata.dtm.jdbc.util.DriverConstants.*;
import static io.arenadata.dtm.jdbc.util.UrlConnectionParser.parseURL;

@Slf4j
public class DtmDriver implements Driver {

    private static final Logger PARENT_LOGGER = LoggerFactory.getLogger("io.arenadata.dtm.jdbc");

    static {
        try {
            DriverManager.registerDriver(new DtmDriver());
            log.info("Driver registered");
        } catch (SQLException e) {
            log.error("Error registering JDBC driver", e.getCause());
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        Properties props = new Properties(info);
        if ((props = parseURL(url, props)) == null) {
            return null;
        }

        return makeConnection(url, props);
    }

    private static Connection makeConnection(String url, Properties info) throws SQLException {
        return new DtmConnectionImpl(dbHost(info), user(info), schema(info), info, url);
    }

    private static String schema(Properties info) {
        return info.getProperty(SCHEMA_PROPERTY, "");
    }

    private static String user(Properties info) {
        return info.getProperty(USER_PROPERTY, "");
    }

    private static String dbHost(Properties info) {
        return info.getProperty(HOST_PROPERTY, "");
    }

    @Override
    public boolean acceptsURL(String url) {
        return parseURL(url, null) != null;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return DriverInfo.MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return DriverInfo.MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
        return (java.util.logging.Logger) PARENT_LOGGER;
    }
}
