package io.arenadata.dtm.jdbc;

import io.arenadata.dtm.jdbc.core.BaseConnection;
import io.arenadata.dtm.jdbc.ext.DtmConnectionImpl;
import io.arenadata.dtm.jdbc.ext.DtmPreparedStatement;
import io.arenadata.dtm.jdbc.ext.DtmResultSet;
import io.arenadata.dtm.jdbc.ext.DtmStatement;

import java.sql.*;

public class DtmDriverCases {

    public static void main(String[] args) throws SQLException {
        String host = "10.92.3.86:9094";
        String user = "";
        String schema = "";
        String url = String.format("jdbc:adtm://%s/", host);
        BaseConnection conn = new DtmConnectionImpl(host, user, schema, null, url);
        ResultSet resultSet = conn.prepareStatement("select * from information_schema.schemata").executeQuery();
//        DtmStatement stmnt = (DtmStatement) conn.createStatement();
//        DatabaseMetaData metaData = conn.getMetaData();
//        ResultSet resultSet = stmnt.executeQuery("use dtm_1106");
        //ResultSet resultSet = metaData.getSchemas();
        //ResultSet resultSet = metaData.getColumns("dtm_1012", "", "accounts_all", null);
        final ResultSet resultSet2 = testPrepareStmnt(conn);
        //final ResultSet resultSet = testStmnt(conn);
        System.out.println(resultSet2);
    }

    private static ResultSet testStmnt(BaseConnection conn) throws SQLException {
        String sql = "select t1.* from dtm_1046.accounts t1 datasource_type='ADB'";
        DtmStatement stmnt = (DtmStatement) conn.createStatement();
        return stmnt.executeQuery(sql);
    }

    private static ResultSet testPrepareStmnt(BaseConnection conn) throws SQLException {
        final String sql = "select t1.account_id as id6 from dtm_1106.accounts t1 " +
                "where t1.account_id = ? datasource_type = 'adb'";

        DtmPreparedStatement stmnt = (DtmPreparedStatement) conn.prepareStatement(sql);
        stmnt.setLong(0, 1);
        ParameterMetaData parameterMetaData = stmnt.getParameterMetaData();
        final ResultSet resultSet = stmnt.executeQuery();
        return resultSet;
    }
}
