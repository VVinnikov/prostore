package io.arenadata.dtm.jdbc.core;

import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.logging.Logger;

@Slf4j
public class DtmDataSource implements DataSource {

    private String[] serverNames = new String[]{"localhost"};
    private String databaseName = "";
    private String user;
    private String password;
    private int[] portNumbers = new int[]{0};

    public Connection getConnection() throws SQLException {
        return getConnection(user, password);
    }

    public Connection getConnection(String user, String password)
            throws SQLException {
        try {
            Connection con = DriverManager.getConnection(getUrl(), user, password);
            log.info("Created connection");
            return con;
        } catch (SQLException e) {
            log.error("Failed to create a connection", e);
            throw e;
        }
    }

    public String getServerName() {
        return serverNames[0];
    }

    public String[] getServerNames() {
        return serverNames;
    }

    public void setServerName(String serverName) {
        this.setServerNames(new String[]{serverName});
    }

    public void setServerNames(String[] serverNames) {
        if (serverNames == null || serverNames.length == 0) {
            this.serverNames = new String[] {"localhost"};
        } else {
            serverNames = serverNames.clone();
            for (int i = 0; i < serverNames.length; i++) {
                String serverName = serverNames[i];
                if (serverName == null || serverName.equals("")) {
                    serverNames[i] = "localhost";
                }
            }
            this.serverNames = serverNames;
        }
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getPortNumber() {
        if (portNumbers == null || portNumbers.length == 0) {
            return 0;
        }
        return portNumbers[0];
    }

    public int[] getPortNumbers() {
        return portNumbers;
    }

    public void setPortNumber(int portNumber) {
        setPortNumbers(new int[] { portNumber });
    }

    public void setPortNumbers(int[] portNumbers) {
        if (portNumbers == null || portNumbers.length == 0) {
            portNumbers = new int[]{0};
        }
        this.portNumbers = Arrays.copyOf(portNumbers, portNumbers.length);
    }

    public String getUrl() {
        StringBuilder url = new StringBuilder(100);
        url.append("jdbc:adtm://");
        for (int i = 0; i < serverNames.length; i++) {
            if (i > 0) {
                url.append(",");
            }
            url.append(serverNames[i]);
            if (portNumbers != null && portNumbers.length >= i && portNumbers[i] != 0) {
                url.append(":").append(portNumbers[i]);
            }
        }
        url.append("/");
        if (databaseName != null) {
            url.append(databaseName);
        }
        return url.toString();
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {

    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {

    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
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
