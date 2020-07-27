package ru.ibs.dtm.query.execution.plugin.adb;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.plugin.api.DtmDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType;
import ru.ibs.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;
import ru.ibs.dtm.query.execution.plugin.api.status.StatusRequestContext;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@SpringBootTest(classes = DtmTestConfiguration.class)
@ExtendWith(VertxExtension.class)
class AdbDtmDataSourcePluginIT {

    @Autowired
    private DdlService ddlService;

    private DtmDataSourcePlugin plugin = new DtmDataSourcePlugin() {

        @Override
        public boolean supports(SourceType sourceType) {
            return false;
        }

        @Override
        public SourceType getSourceType() {
            return SourceType.ADB;
        }

        @Override
        public void ddl(DdlRequestContext ddlRequest, Handler<AsyncResult<Void>> handler) {
            ddlService.execute(ddlRequest, handler);
        }

        @Override
        public void llr(LlrRequestContext llrRequest, Handler<AsyncResult<QueryResult>> handler) {

        }

        @Override
        public void mpprKafka(MpprRequestContext mpprRequest, Handler<AsyncResult<QueryResult>> handler) {

        }

        @Override
        public void mppwKafka(MppwRequestContext mppwRequest, Handler<AsyncResult<QueryResult>> handler) {

        }

        @Override
        public void calcQueryCost(QueryCostRequestContext queryCostRequest, Handler<AsyncResult<Integer>> handler) {

        }

        @Override
        public void status(StatusRequestContext statusRequestContext, Handler<AsyncResult<StatusQueryResult>> asyncResultHandler) {

        }
    };

    @Test
    void testDdl(VertxTestContext testContext) throws Throwable {
        ClassTable classTable = new ClassTable("test.test_ts3222", Arrays.asList(
                new ClassField(0, "id", ColumnType.INT.name(), false, 1, 1, null),
                new ClassField(1, "name", ColumnType.VARCHAR.name(), true, null, null, null),
                new ClassField(2, "dt", ColumnType.TIMESTAMP.name(), true, null, null, null)
        ));
        DdlRequest dto = new DdlRequest(null, classTable);
        DdlRequestContext context = new DdlRequestContext(dto);
        context.setDdlType(DdlType.CREATE_TABLE);
        plugin.ddl(context, ar -> {
            if (ar.succeeded()) {
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }
}
