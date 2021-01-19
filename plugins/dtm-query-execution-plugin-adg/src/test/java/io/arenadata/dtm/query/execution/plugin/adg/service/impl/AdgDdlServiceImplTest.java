package io.arenadata.dtm.query.execution.plugin.adg.service.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adg.service.impl.ddl.AdgDdlService;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

// FixMe Test
class AdgDdlServiceImplTest {
//    private final AdgDdlService adgDdlService = new AdgDdlService();
//
//    @Test
//    void testExecuteNotEmptyOk() {
//        Promise<Void> promise = Promise.promise();
//        QueryRequest queryRequest = new QueryRequest();
//        Entity entity = new Entity("test_schema.test_table", Collections.emptyList());
//        DdlRequestContext context = new DdlRequestContext(new DdlRequest(queryRequest, entity));
//        context.setDdlType(DROP_TABLE);
//        SqlNode sqlNode = mock(SqlNode.class);
//        when(sqlNode.getKind()).thenReturn(SqlKind.DROP_TABLE);
//        context.setQuery(sqlNode);
//        DdlExecutor executor = mock(DdlExecutor.class);
//        when(executor.getSqlKind()).thenReturn(SqlKind.DROP_TABLE);
//        when(executor.execute(any(), any())).thenReturn(Future.succeededFuture());
//        adgDdlService.addExecutor(executor);
//        adgDdlService.execute(context)
//                .onComplete(promise);
//        assertTrue(promise.future().succeeded());
//    }
//
//    @Test
//    void testDdlQueryIsNull() {
//        Promise<Void> promise = Promise.promise();
//        QueryRequest queryRequest = new QueryRequest();
//        Entity entity = new Entity("test_schema.test_table", Collections.emptyList());
//        DdlRequestContext context = new DdlRequestContext(new DdlRequest(queryRequest, entity));
//        context.setDdlType(DROP_TABLE);
//        adgDdlService.execute(context)
//                .onComplete(promise);
//        assertEquals(promise.future().cause().getMessage(), "Ddl query is null!");
//    }
//
//    @Test
//    void testUnknownSqlKind() {
//        Promise<Void> promise = Promise.promise();
//        QueryRequest queryRequest = new QueryRequest();
//        Entity entity = new Entity("test_schema.test_table", Collections.emptyList());
//        DdlRequestContext context = new DdlRequestContext(new DdlRequest(queryRequest, entity));
//        context.setDdlType(DROP_TABLE);
//        SqlNode sqlNode = mock(SqlNode.class);
//        when(sqlNode.getKind()).thenReturn(SqlKind.DROP_TABLE);
//        context.setQuery(sqlNode);
//        adgDdlService.execute(context)
//                .onComplete(promise);
//        assertEquals(promise.future().cause().getMessage(), "Unknown DDL: DROP_TABLE");
//    }
}
