package io.arenadata.dtm.jdbc;

import io.arenadata.dtm.jdbc.core.BaseConnection;
import io.arenadata.dtm.jdbc.ext.DtmConnectionImpl;
import io.arenadata.dtm.jdbc.ext.DtmPreparedStatement;

import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;

public class DtmDriverCases {

    public static void main(String[] args) throws SQLException {
        String host = "localhost:9090";
        String user = "";
        String schema = "";
        String url = String.format("jdbc:adtm://%s/", host);

        BaseConnection conn = new DtmConnectionImpl(host, user, schema, null, url);
        //1605647472000
        //1605629472000
        final String sql = "select * from dtm_889.all_types_table " +
                "where id = ? " +
                " and double_col = ?" +
                " and float_col = ?" +
                " and varchar_col = ?" +
                " and boolean_col = ?" +
                " and int_col = ?" +
                " and bigint_col = ?" +
                " and date_col = ?" +
                " and timestamp_col = ?" +
                " and time_col = ?" +
                " and uuid_col = ?" +
                " and char_col = ?" +
                " datasource_type='adg'";

        DtmPreparedStatement stmnt = (DtmPreparedStatement) conn.prepareStatement(sql);
        stmnt.setInt(0, 1);
        stmnt.setDouble(1, 1d);
        stmnt.setFloat(2, 1.0f);
        stmnt.setString(3, "sss");
        stmnt.setBoolean(4, true);
        stmnt.setInt(5, 1);
        stmnt.setLong(6, 100000L);
        stmnt.setDate(7, Date.valueOf(LocalDate.ofEpochDay(1605271)));//6365-01-31 00:00:000+0500
        //138695310000000
        //1605271 -ADG
        //1605647472000000
        //Date.valueOf(LocalDate.ofEpochDay(1605271))
        Date.valueOf(LocalDateTime.ofInstant(Instant.ofEpochMilli(138695396400000L), ZoneId.of("UTC")).toLocalDate()).toInstant();
        //LocalDateTime.ofInstant(Instant.ofEpochMilli(1605647472000L), ZoneId.of("UTC"));
        //LocalDateTime.ofInstant(Instant.ofEpochMilli(1605629472000L), ZoneId.of("UTC"));
        stmnt.setTimestamp(8,  Timestamp.valueOf("2020-11-17 21:11:12"));
        stmnt.setTime(9, Time.valueOf("00:01:40"));
        stmnt.setString(10, "d92beee8-749f-4539-aa15-3d2941dbb0f1");
        stmnt.setString(11, "c");
        final ResultSet resultSet = stmnt.executeQuery();
        System.out.println(resultSet);
    }
}
