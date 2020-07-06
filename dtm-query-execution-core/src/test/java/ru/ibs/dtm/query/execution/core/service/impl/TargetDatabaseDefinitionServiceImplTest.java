package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.junit.jupiter.api.Test;
import org.springframework.plugin.core.SimplePluginRegistry;
import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.QuerySourceRequest;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.core.service.SchemaStorageProvider;
import ru.ibs.dtm.query.execution.core.service.TargetDatabaseDefinitionService;
import ru.ibs.dtm.query.execution.plugin.api.DtmDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.status.StatusRequestContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class TargetDatabaseDefinitionServiceImplTest {

    private TargetDatabaseDefinitionService targetDatabaseDefinitionService =
            new TargetDatabaseDefinitionServiceImpl(
                    mock(SchemaStorageProvider.class),
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
                                        public void mpprKafka(MpprRequestContext request, Handler<AsyncResult<QueryResult>> handler) {
                                        }

                                        @Override
                                        public void mppwKafka(MppwRequestContext request, Handler<AsyncResult<QueryResult>> handler) {
                                        }

                                        @Override
                                        public void calcQueryCost(QueryCostRequestContext request, Handler<AsyncResult<Integer>> handler) {
                                            handler.handle(Future.succeededFuture(0));
                                        }

                                        @Override
                                        public void status(StatusRequestContext statusRequestContext, Handler<AsyncResult<StatusQueryResult>> asyncResultHandler) {

                                        }
                                    }
                            )
                    ));

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
