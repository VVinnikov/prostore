package io.arenadata.dtm.query.execution.core.service.impl;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.configuration.properties.VertxPoolProperties;
import io.arenadata.dtm.query.execution.core.service.TargetDatabaseDefinitionService;
import io.arenadata.dtm.query.execution.core.service.schema.impl.LogicalSchemaProviderImpl;
import io.arenadata.dtm.query.execution.core.verticle.impl.TaskVerticleExecutorImpl;
import io.arenadata.dtm.query.execution.plugin.api.DtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.status.StatusRequestContext;
import io.vertx.core.*;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.plugin.core.SimplePluginRegistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@Disabled
public class TargetDatabaseDefinitionServiceImplTest {

    private TargetDatabaseDefinitionService targetDatabaseDefinitionService;

    {
        TaskVerticleExecutorImpl workerVerticleExecutor = new TaskVerticleExecutorImpl(new VertxPoolProperties());
        Vertx.vertx().deployVerticle(workerVerticleExecutor);
        targetDatabaseDefinitionService = new TargetDatabaseDefinitionServiceImpl(
            mock(LogicalSchemaProviderImpl.class),
            new DataSourcePluginServiceImpl(
                SimplePluginRegistry.of(
                    new DtmDataSourcePlugin() {

                        @Override
                        public SourceType getSourceType() {
                            return SourceType.ADB;
                        }

                        @Override
                        public void ddl(DdlRequestContext context, Handler<AsyncResult<Void>> handler) {

                        }

                        @Override
                        public void llr(LlrRequestContext request, Handler<AsyncResult<QueryResult>> handler) {
                        }

                        @Override
                        public void mppr(MpprRequestContext request, Handler<AsyncResult<QueryResult>> handler) {
                        }

                        @Override
                        public void mppw(MppwRequestContext request, Handler<AsyncResult<QueryResult>> handler) {
                        }

                        @Override
                        public void calcQueryCost(QueryCostRequestContext request, Handler<AsyncResult<Integer>> handler) {
                            handler.handle(Future.succeededFuture(0));
                        }

                        @Override
                        public void status(StatusRequestContext context, Handler<AsyncResult<StatusQueryResult>> asyncResultHandler) {

                        }

                        @Override
                        public void rollback(RollbackRequestContext context, Handler<AsyncResult<Void>> asyncResultHandler) {

                        }
                    }
                ),
                workerVerticleExecutor));
    }

    @Test
    void getTargetSourceOk() {
        Promise promise = Promise.promise();
        QueryRequest request = new QueryRequest();
        request.setSql("select * from dual");

        targetDatabaseDefinitionService.getTargetSource(new QuerySourceRequest(request, SourceType.ADB), handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result().getSourceType());
            } else {
                promise.fail(handler.cause());
            }
        });

        assertEquals(SourceType.ADB, promise.future().result());
    }

    @Test
    void getTargetSourceWhenHintExist() {
        Promise promise = Promise.promise();
        QueryRequest request = new QueryRequest();
        request.setSql("select * from dual");

        targetDatabaseDefinitionService.getTargetSource(new QuerySourceRequest(request, SourceType.ADG), handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result().getSourceType());
            } else {
                promise.fail(handler.cause());
            }
        });

        assertEquals(SourceType.ADG, promise.future().result());
    }

    @Test
    void getTargetSourceInformationSchema() {
        QueryRequest request = new QueryRequest();
        request.setSql("select * from information_schema.schemata");

        targetDatabaseDefinitionService.getTargetSource(new QuerySourceRequest(request, null), handler -> {
            assertTrue(handler.succeeded());
            assertEquals(SourceType.INFORMATION_SCHEMA, handler.result().getSourceType());
        });
    }

    @Test
    void getTargetSourceInformationSchemaWithQuotes() {
        QueryRequest request = new QueryRequest();
        request.setSql("select * from \"information_schema\".\"schemata\"");

        targetDatabaseDefinitionService.getTargetSource(new QuerySourceRequest(request, null), handler -> {
            assertTrue(handler.succeeded());
            assertEquals(SourceType.INFORMATION_SCHEMA, handler.result().getSourceType());
        });
    }
}