package ru.ibs.dtm.jdbc;

import ru.ibs.dtm.jdbc.ext.DtmConnection;
import ru.ibs.dtm.jdbc.ext.DtmStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DtmDriverTest {

    public static void main(String[] args) {
        String host = "localhost:8088";
        String user = "";
        String schema = "";
        String url = String.format("jdbc:adtm://%s/", host);

        DtmConnection conn = new DtmConnection(host, user, schema, null, url);
        Statement stmnt = new DtmStatement(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        try {
            final String dbName = "db" + String.valueOf(Math.random()).replaceAll("\\.", "");
            stmnt.execute("CREATE DATABASE " + dbName);
            stmnt.execute("DROP DATABASE " + dbName);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
