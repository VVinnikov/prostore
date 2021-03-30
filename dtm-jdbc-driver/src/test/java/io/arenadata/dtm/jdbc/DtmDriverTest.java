package io.arenadata.dtm.jdbc;

import io.arenadata.dtm.jdbc.core.BaseConnection;
import io.arenadata.dtm.jdbc.ext.DtmConnectionImpl;
import io.arenadata.dtm.jdbc.ext.DtmStatement;
import lombok.SneakyThrows;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class DtmDriverTest {

    public static void main(String[] args) throws SQLException, InterruptedException {
        String host = "localhost:9090";
        String user = "";
        String schema = "";
        String url = String.format("jdbc:adtm://%s/", host);
        AtomicLong t = new AtomicLong();
        int count = 3;
        long total = System.currentTimeMillis();
        for (int j = 0; j < 100; j++) {
            CountDownLatch latch = new CountDownLatch(count);
            for (int i = 0; i < count; i++) {
                int finalI = i;
                new Thread(() -> {
                    try {
                        long tt = System.currentTimeMillis();
                        extracted(host, user, schema, url, finalI);
                        t.addAndGet(System.currentTimeMillis() - tt);
                        latch.countDown();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }).start();
            }
            latch.await();
        }
        System.out.printf("avg %d; total %d%n", (t.get() / count), System.currentTimeMillis() - total);
    }

    @SneakyThrows
    private static void extracted(String host,
                                  String user,
                                  String schema,
                                  String url,
                                  int i) throws SQLException {
        try (BaseConnection conn = new DtmConnectionImpl(host, user, schema, null, url)) {
            DtmStatement stmnt = (DtmStatement) conn.createStatement();
            ResultSet resultSet = stmnt.executeQuery("select * from dtm_866.test_table where id = " + i);
            System.out.println(resultSet);
        }
    }
}
