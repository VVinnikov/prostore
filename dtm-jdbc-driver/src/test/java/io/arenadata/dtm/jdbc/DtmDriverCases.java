package io.arenadata.dtm.jdbc;

import io.arenadata.dtm.jdbc.core.BaseConnection;
import io.arenadata.dtm.jdbc.ext.DtmConnectionImpl;
import io.arenadata.dtm.jdbc.ext.DtmDatabaseMetaData;
import io.arenadata.dtm.jdbc.ext.DtmStatement;
import io.arenadata.dtm.jdbc.model.TableInfo;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class DtmDriverCases {

    public static void main(String[] args) throws SQLException {
        String host = "localhost:9090";
        String user = "";
        String schema = "";
        String url = String.format("jdbc:adtm://%s/", host);

        BaseConnection conn = new DtmConnectionImpl(host, user, schema, null, url);
        DtmStatement stmnt = (DtmStatement) conn.createStatement();
        DtmStatement stmnt2 = (DtmStatement) conn.createStatement();
        final List<TableInfo> tables = conn.getQueryExecutor().getTables("dtm_714");
        final ResultSet createDb = stmnt.executeQuery("CREATE DATABASE dtm_test_sql;");
        final ResultSet dropDb = stmnt.executeQuery("DROP DATABASE dtm_test_sql;");
        final ResultSet resultSet1 = stmnt.executeQuery("USE dtm_714");
        //final ResultSet resultSet2 = stmnt.executeQuery("get_delta_ok(); get_delta_ok();");
        //resultSet2.getObject(2);
        DtmDatabaseMetaData dtmDatabaseMetaData = new DtmDatabaseMetaData(conn);
        //final ResultSet columns = dtmDatabaseMetaData.getColumns("dtm_579", "%", "transactions_2", "%");
    }
}
