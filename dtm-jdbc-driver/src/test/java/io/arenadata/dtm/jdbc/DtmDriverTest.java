package io.arenadata.dtm.jdbc;

import io.arenadata.dtm.jdbc.ext.DtmConnection;
import io.arenadata.dtm.jdbc.ext.DtmDatabaseMetaData;
import io.arenadata.dtm.jdbc.ext.DtmStatement;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DtmDriverTest {

    public static void main(String[] args) throws SQLException {
        String host = "localhost:9090";
        String user = "";
        String schema = "";
        String url = String.format("jdbc:adtm://%s/", host);

        DtmConnection conn = new DtmConnection(host, user, schema, null, url);
        DtmStatement stmnt = (DtmStatement) conn.createStatement();
        DtmStatement stmnt2 = (DtmStatement) conn.createStatement();
        final ResultSet resultSet1 = stmnt.executeQuery("USE dtm_687");
        final ResultSet resultSet2 = stmnt.executeQuery("select * from dtm_687.transactions");
        resultSet2.getObject(3);
        DtmDatabaseMetaData dtmDatabaseMetaData = new DtmDatabaseMetaData(conn);
        //final ResultSet columns = dtmDatabaseMetaData.getColumns("dtm_579", "%", "transactions_2", "%");
    }
}
