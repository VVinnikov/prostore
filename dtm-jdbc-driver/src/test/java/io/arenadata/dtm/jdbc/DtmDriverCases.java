package io.arenadata.dtm.jdbc;

import io.arenadata.dtm.jdbc.core.BaseConnection;
import io.arenadata.dtm.jdbc.ext.DtmConnectionImpl;
import io.arenadata.dtm.jdbc.ext.DtmPreparedStatement;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DtmDriverCases {

    public static void main(String[] args) throws SQLException {
        String host = "localhost:9090";
        String user = "";
        String schema = "";
        String url = String.format("jdbc:adtm://%s/", host);

        BaseConnection conn = new DtmConnectionImpl(host, user, schema, null, url);
        final String sql = "select * from dtm_902.all_types_table where id = ? datasource_type='adqm'";

        DtmPreparedStatement stmnt = (DtmPreparedStatement) conn.prepareStatement(sql);
        stmnt.setInt(0, 1);
       /* stmnt.setDouble(1, 2d);
        stmnt.setBoolean(3, true);
        stmnt.setFloat(4, 1.0f);
        stmnt.setLong(5, 4L);
        stmnt.setString(6, "test");
        stmnt.setDate(7, Date.valueOf(LocalDate.now()));
        stmnt.setTime(8, Time.valueOf(LocalTime
                .parse("14:14:00", DateTimeFormatter.ISO_LOCAL_TIME)));
        stmnt.setTimestamp(9, Timestamp.from(LocalDateTime
                .parse("2021-01-14T14:14:00", DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                .atZone(ZoneId.of("UTC")).toInstant()));*/
        final ResultSet resultSet = stmnt.executeQuery();
    }
}
