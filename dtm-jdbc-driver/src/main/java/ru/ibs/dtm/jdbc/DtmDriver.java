package ru.ibs.dtm.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.ibs.dtm.jdbc.ext.DtmConnection;

import java.sql.*;
import java.util.Properties;

import static ru.ibs.dtm.jdbc.util.DriverConstants.*;
import static ru.ibs.dtm.jdbc.util.UrlConnectionParser.parseURL;

public class DtmDriver implements Driver {

    private static final Logger PARENT_LOGGER = LoggerFactory.getLogger("ru.ibs.dtm.driver.jdbc");
    private static final Logger LOGGER = LoggerFactory.getLogger("ru.ibs.dtm.driver.jdbc.DtmDriver");

    static {
        try {
            DriverManager.registerDriver(new DtmDriver());
            LOGGER.info("Driver registered");
        } catch (SQLException e) {
            LOGGER.error("Error registering JDBC driver " + e.getCause());
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        parseURL(url, info);

        return makeConnection(url, info);
    }

    private static Connection makeConnection(String url, Properties info) {
        return new DtmConnection(dbHost(info), user(info), schema(info), info, url);
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
        return url.startsWith(CONNECT_URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
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
