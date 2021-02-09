package io.arenadata.dtm.jdbc;

import io.arenadata.dtm.jdbc.core.BaseConnection;
import io.arenadata.dtm.jdbc.ext.DtmConnectionImpl;
import io.arenadata.dtm.jdbc.ext.DtmDatabaseMetaData;
import io.arenadata.dtm.jdbc.ext.DtmResultSet;
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
        final DtmResultSet resultSet = (DtmResultSet) stmnt.executeQuery("select * from dtm_928_2.all_types_table where id = 1 datasource_type='adqm'");
        resultSet.getDate(8);
        resultSet.getTimestamp(9);
        resultSet.getTime(10);
        System.out.println(resultSet);
    }
}
