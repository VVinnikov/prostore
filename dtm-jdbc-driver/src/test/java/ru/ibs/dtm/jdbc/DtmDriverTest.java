package ru.ibs.dtm.jdbc;

import ru.ibs.dtm.jdbc.ext.DtmConnection;
import ru.ibs.dtm.jdbc.ext.DtmDatabaseMetaData;
import ru.ibs.dtm.jdbc.ext.DtmStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DtmDriverTest {

    public static void main(String[] args) throws SQLException {
        String host = "localhost:9090";
        String user = "";
        String schema = "";
        String url = String.format("jdbc:adtm://%s/", host);

        DtmConnection conn = new DtmConnection(host, user, schema, null, url);
        DtmDatabaseMetaData dtmDatabaseMetaData = new DtmDatabaseMetaData(conn);
        final ResultSet columns = dtmDatabaseMetaData.getColumns("dtm_579", "%", "transactions_2", "%");
    }
}
