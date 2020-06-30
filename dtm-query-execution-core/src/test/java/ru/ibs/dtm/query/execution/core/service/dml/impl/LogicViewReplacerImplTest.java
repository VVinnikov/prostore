package ru.ibs.dtm.query.execution.core.service.dml.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.dto.DatamartView;
import ru.ibs.dtm.query.execution.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.service.impl.CalciteDefinitionService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

@Slf4j
class LogicViewReplacerImplTest {

    public static final String EXPECTED_WITHOUT_JOIN = "SELECT v.col1 AS c, v.col2 AS r\n" +
            "FROM (SELECT col4, col5\n" +
            "FROM tblx FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14'\n" +
            "WHERE tblx.col6 = 0) AS v";

    public static final String EXPECTED_WITH_JOIN = "SELECT v.col1 AS c, v.col2 AS r\n" +
            "FROM tbl FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' AS t\n" +
            "INNER JOIN (SELECT col4, col5\n" +
            "FROM tblx FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'\n" +
            "WHERE tblx.col6 = 0) AS v ON t.col3 = v.col4";

    public static final String EXPECTED_WITH_JOIN_WITHOUT_ALIAS = "SELECT view.col1 AS c, view.col2 AS r\n" +
            "FROM (SELECT col4, col5\n" +
            "FROM tblx\n" +
            "WHERE tblx.col6 = 0) AS view";

    public static final String EXPECTED_WITH_JOIN_AND_WHERE = "SELECT v.col1 AS c, v.col2 AS r\n" +
            "FROM tbl FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' AS t\n" +
            "INNER JOIN (SELECT col4, col5\n" +
            "FROM tblx FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'\n" +
            "WHERE tblx.col6 = 0) AS v ON t.col3 = v.col4\n" +
            "WHERE EXISTS (SELECT id\n" +
            "FROM (SELECT col4, col5\n" +
            "FROM tblx\n" +
            "WHERE tblx.col6 = 0) AS view)";

    public static final String EXPECTED_WITH_SELECT = "SELECT t.col1 AS c, (SELECT id\n" +
            "FROM (SELECT col4, col5\n" +
            "FROM tblx\n" +
            "WHERE tblx.col6 = 0) AS view\n" +
            "FETCH NEXT 1 ROWS ONLY) AS r\n" +
            "FROM tblt AS t";

    public static final String EXPECTED_WITH_DATAMART = "SELECT v.col1 AS c, v.col2 AS r\n" +
            "FROM (SELECT col4, col5\n" +
            "FROM tblx\n" +
            "WHERE tblx.col6 = 0) AS v";

    public static final String EXPECTED_WITH_VIEW_IN_VIEW = "SELECT v.col1 AS c, v.col2 AS r\n" +
            "FROM (SELECT col4, col5\n" +
            "FROM (SELECT col4, col5\n" +
            "FROM tblc FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14'\n" +
            "WHERE tblc.col9 = 0) AS tblz\n" +
            "WHERE tblz.col6 = 0) AS v";

    private final CalciteConfiguration config = new CalciteConfiguration();
    private final DefinitionService<SqlNode> definitionService =
            new CalciteDefinitionService(config.configEddlParser(config.eddlParserImplFactory()));

    @Test
    void withoutJoin() throws InterruptedException {
        val testContext = new VertxTestContext();
        val serviceDao = mock(ServiceDao.class);
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<DatamartView>>> handler = invocation.getArgument(2);
            val vq1 = "SELECT Col4, Col5 \n" +
                    "FROM tblX \n" +
                    "WHERE tblX.Col6 = 0";
            val v1 = new DatamartView("view", 1, vq1);
            handler.handle(Future.succeededFuture(Collections.singletonList(v1)));
            return null;
        }).when(serviceDao).findViewsByDatamart(any(), any(), any());
        val loader = new DatamartViewWrapLoaderImpl(serviceDao);
        val replacer = new LogicViewReplacerImpl(definitionService, new SqlSnapshotReplacerImpl(), loader);
        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM view FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' v";
        replacer.replace(sql, "datamart", sqlResult -> {
            if (sqlResult.succeeded()) {
                assertEquals(EXPECTED_WITHOUT_JOIN, sqlResult.result());
                testContext.completeNow();
            } else {
                testContext.failNow(sqlResult.cause());
            }
        });
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void withDatamart() throws InterruptedException {
        val testContext = new VertxTestContext();
        val serviceDao = mock(ServiceDao.class);
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<DatamartView>>> handler = invocation.getArgument(2);
            val vq1 = "SELECT Col4, Col5 \n" +
                    "FROM tblX \n" +
                    "WHERE tblX.Col6 = 0";
            val v1 = new DatamartView("view", 1, vq1);
            handler.handle(Future.succeededFuture(Collections.singletonList(v1)));
            return null;
        }).when(serviceDao).findViewsByDatamart(any(), any(), any());
        val loader = new DatamartViewWrapLoaderImpl(serviceDao);
        val replacer = new LogicViewReplacerImpl(definitionService, new SqlSnapshotReplacerImpl(), loader);
        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM test.view v";
        replacer.replace(sql, "datamart", sqlResult -> {
            if (sqlResult.succeeded()) {
                assertEquals(EXPECTED_WITH_DATAMART, sqlResult.result());
                testContext.completeNow();
            } else {
                testContext.failNow(sqlResult.cause());
            }
        });
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void withoutJoin_withoutAlias() throws InterruptedException {
        val testContext = new VertxTestContext();
        val serviceDao = mock(ServiceDao.class);
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<DatamartView>>> handler = invocation.getArgument(2);
            val vq1 = "SELECT Col4, Col5 \n" +
                    "FROM tblX \n" +
                    "WHERE tblX.Col6 = 0";
            val v1 = new DatamartView("view", 1, vq1);
            handler.handle(Future.succeededFuture(Collections.singletonList(v1)));
            return null;
        }).when(serviceDao).findViewsByDatamart(any(), any(), any());
        val loader = new DatamartViewWrapLoaderImpl(serviceDao);
        val replacer = new LogicViewReplacerImpl(definitionService, new SqlSnapshotReplacerImpl(), loader);
        val sql = "SELECT view.Col1 as c, view.Col2 r\n" +
                "FROM view";
        replacer.replace(sql, "datamart", sqlResult -> {
            if (sqlResult.succeeded()) {
                assertEquals(EXPECTED_WITH_JOIN_WITHOUT_ALIAS, sqlResult.result());
                testContext.completeNow();
            } else {
                testContext.failNow(sqlResult.cause());
            }
        });
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void withJoin() throws InterruptedException {
        val testContext = new VertxTestContext();
        val serviceDao = mock(ServiceDao.class);
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<DatamartView>>> handler = invocation.getArgument(2);
            val vq1 = "SELECT Col4, Col5 \n" +
                    "FROM tblX \n" +
                    "WHERE tblX.Col6 = 0";
            val v1 = new DatamartView("view", 1, vq1);
            handler.handle(Future.succeededFuture(Collections.singletonList(v1)));
            return null;
        }).when(serviceDao).findViewsByDatamart(any(), any(), any());
        val loader = new DatamartViewWrapLoaderImpl(serviceDao);
        val replacer = new LogicViewReplacerImpl(definitionService, new SqlSnapshotReplacerImpl(), loader);
        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM tbl FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' t\n" +
                "JOIN view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' v\n" +
                "ON t.Col3 = v.Col4";
        replacer.replace(sql, "datamart", sqlResult -> {
            if (sqlResult.succeeded()) {
                assertEquals(EXPECTED_WITH_JOIN, sqlResult.result());
                testContext.completeNow();
            } else {
                testContext.failNow(sqlResult.cause());
            }
        });
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void withJoinAndWhere() throws InterruptedException {
        val testContext = new VertxTestContext();
        val serviceDao = mock(ServiceDao.class);
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<DatamartView>>> handler = invocation.getArgument(2);
            val vq1 = "SELECT Col4, Col5 \n" +
                    "FROM tblX \n" +
                    "WHERE tblX.Col6 = 0";
            val v1 = new DatamartView("view", 1, vq1);
            handler.handle(Future.succeededFuture(Collections.singletonList(v1)));
            return null;
        }).when(serviceDao).findViewsByDatamart(any(), any(), any());
        val loader = new DatamartViewWrapLoaderImpl(serviceDao);
        val replacer = new LogicViewReplacerImpl(definitionService, new SqlSnapshotReplacerImpl(), loader);
        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM tbl FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' t\n" +
                "JOIN view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' v\n" +
                "ON t.Col3 = v.Col4 \n" +
                "WHERE exists (select id from view)";
        replacer.replace(sql, "datamart", sqlResult -> {
            if (sqlResult.succeeded()) {
                assertEquals(EXPECTED_WITH_JOIN_AND_WHERE, sqlResult.result());
                testContext.completeNow();
            } else {
                testContext.failNow(sqlResult.cause());
            }
        });
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void withJoinAndSelect() throws InterruptedException {
        val testContext = new VertxTestContext();
        val serviceDao = mock(ServiceDao.class);
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<DatamartView>>> handler = invocation.getArgument(2);
            val vq1 = "SELECT Col4, Col5 \n" +
                    "FROM tblX \n" +
                    "WHERE tblX.Col6 = 0";
            val v1 = new DatamartView("view", 1, vq1);
            handler.handle(Future.succeededFuture(Collections.singletonList(v1)));
            return null;
        }).when(serviceDao).findViewsByDatamart(any(), any(), any());
        val loader = new DatamartViewWrapLoaderImpl(serviceDao);
        val replacer = new LogicViewReplacerImpl(definitionService, new SqlSnapshotReplacerImpl(), loader);
        val sql = "SELECT t.Col1 as c, (select id from view limit 1) r\n" +
                "FROM tblt t";
        replacer.replace(sql, "datamart", sqlResult -> {
            if (sqlResult.succeeded()) {
                assertEquals(EXPECTED_WITH_SELECT, sqlResult.result());
                testContext.completeNow();
            } else {
                testContext.failNow(sqlResult.cause());
            }
        });
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void viewInView() throws InterruptedException {
        val testContext = new VertxTestContext();
        val serviceDao = mock(ServiceDao.class);
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<DatamartView>>> handler = invocation.getArgument(2);
            val views = new ArrayList<DatamartView>();
            views.add(new DatamartView(
                    "view", 1, "SELECT Col4, Col5 \n" +
                    "FROM tblZ \n" +
                    "WHERE tblZ.Col6 = 0"));
            views.add(new DatamartView(
                    "tblZ", 1, "SELECT Col4, Col5 \n" +
                    "FROM tblC \n" +
                    "WHERE tblC.Col9 = 0"));
            handler.handle(Future.succeededFuture(views));
            return null;
        }).when(serviceDao).findViewsByDatamart(any(), any(), any());
        val loader = new DatamartViewWrapLoaderImpl(serviceDao);
        val replacer = new LogicViewReplacerImpl(definitionService, new SqlSnapshotReplacerImpl(), loader);
        val sql = "SELECT v.Col1 as c, v.Col2 r\n" +
                "FROM view FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' v";
        replacer.replace(sql, "datamart", sqlResult -> {
            if (sqlResult.succeeded()) {
                assertEquals(EXPECTED_WITH_VIEW_IN_VIEW, sqlResult.result());
                testContext.completeNow();
            } else {
                testContext.failNow(sqlResult.cause());
            }
        });
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }
}
