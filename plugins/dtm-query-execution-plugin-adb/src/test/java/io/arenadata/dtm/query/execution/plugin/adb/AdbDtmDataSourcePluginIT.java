package io.arenadata.dtm.query.execution.plugin.adb;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.api.DtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlType;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Request;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprPluginRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwPluginRequest;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SpringBootTest(classes = DtmTestConfiguration.class)
@ExtendWith(VertxExtension.class)
public class AdbDtmDataSourcePluginIT {

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
        public Future<Void> ddl(DdlRequestContext ddlRequest) {
            return ddlService.execute(ddlRequest);
        }

        @Override
        public Future<QueryResult> llr(LlrRequest llrRequest) {
            return null;
        }

        @Override
        public Future<QueryResult> mppr(MpprPluginRequest request) {
            return null;
        }

        @Override
        public Future<QueryResult> mppw(MppwPluginRequest mppwRequest) {
            return null;
        }

        @Override
        public Future<Integer> calcQueryCost(QueryCostRequestContext queryCostRequest) {
            return null;
        }

        @Override
        public Future<StatusQueryResult> status(String topic) {
            return null;
        }

        @Override
        public Future<Void> rollback(RollbackRequestContext context) {
            return null;
        }

        @Override
        public Set<String> getActiveCaches() {
            return Collections.singleton("adb_datamart");
        }

        @Override
        public Future<Void> checkTable(CheckContext context) {
            return null;
        }

        @Override
        public Future<Long> checkDataByCount(CheckDataByCountRequest request) {
            return null;
        }

        @Override
        public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Request params) {
            return null;
        }

        @Override
        public Future<Void> truncateHistory(TruncateHistoryRequest params) {
            return null;
        }
    };

    @Test
    void testDdl(VertxTestContext testContext) throws Throwable {
        Entity entity = new Entity("test.test_ts3222", Arrays.asList(
                new EntityField(0, "id", ColumnType.INT.name(), false, 1, 1, null),
                new EntityField(1, "name", ColumnType.VARCHAR.name(), true, null, null, null),
                new EntityField(2, "dt", ColumnType.TIMESTAMP.name(), true, null, null, null)
        ));
        DdlRequest dto = new DdlRequest(null, entity);
        DdlRequestContext context = new DdlRequestContext(dto);
        context.setDdlType(DdlType.CREATE_TABLE);
        plugin.ddl(context)
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        testContext.completeNow();
                    } else {
                        testContext.failNow(ar.cause());
                    }
                });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }
}
