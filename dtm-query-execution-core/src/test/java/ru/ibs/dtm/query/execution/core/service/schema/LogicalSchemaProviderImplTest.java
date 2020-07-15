package ru.ibs.dtm.query.execution.core.service.schema;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.dto.schema.DatamartSchemaKey;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.model.metadata.DatamartTable;
import ru.ibs.dtm.query.execution.model.metadata.TableAttribute;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

class LogicalSchemaProviderImplTest {

    private final CalciteConfiguration config = new CalciteConfiguration();
    private final LogicalSchemaService logicalSchemaService = mock(LogicalSchemaServiceImpl.class);
    private LogicalSchemaProvider logicalSchemaProvider;
    private QueryRequest queryRequest;

    @BeforeEach
    void setUp() {
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test_datamart");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        queryRequest.setSubRequestId("6efad624-b9da-4ba1-9fed-f2da478b08e8");
        logicalSchemaProvider = new LogicalSchemaProviderImpl(logicalSchemaService);
    }

    @Test
    void createSchemaSuccess() {
        Promise promise = Promise.promise();
        final Map<DatamartSchemaKey, DatamartTable> datamartTableMap = new HashMap<>();
        DatamartTable table1 = new DatamartTable();
        table1.setId(UUID.randomUUID());
        table1.setSchema("test");
        table1.setLabel("pso");
        TableAttribute attr = new TableAttribute();
        attr.setId(UUID.randomUUID());
        attr.setMnemonic("id");
        attr.setLength(null);
        attr.setAccuracy(null);
        attr.setPrimaryKeyOrder(1);
        attr.setDistributeKeyOrder(1);
        table1.setTableAttributes(Collections.singletonList(attr));
        TableAttribute attr2 = new TableAttribute();
        attr2.setId(UUID.randomUUID());
        attr2.setMnemonic("id");
        attr2.setLength(10);
        attr2.setAccuracy(null);
        attr2.setPrimaryKeyOrder(1);
        attr2.setDistributeKeyOrder(1);
        DatamartTable table2 = new DatamartTable();
        table2.setId(UUID.randomUUID());
        table2.setSchema("test");
        table2.setLabel("doc");
        table2.setTableAttributes(Collections.singletonList(attr2));
        datamartTableMap.put(new DatamartSchemaKey("test", "doc"), table2);
        datamartTableMap.put(new DatamartSchemaKey("test", "pso"), table1);

        List<Datamart> datamarts = new ArrayList<>();
        Datamart dm = new Datamart();
        dm.setId(UUID.randomUUID());
        dm.setMnemonic("test");
        dm.setDatamartTables(Arrays.asList(table2, table1));
        datamarts.add(dm);
        doAnswer(invocation -> {
            final Handler<AsyncResult<Map<DatamartSchemaKey, DatamartTable>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartTableMap));
            return null;
        }).when(logicalSchemaService).createSchema(any(), any());

        logicalSchemaProvider.getSchema(queryRequest, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertEquals(datamarts.get(0).getMnemonic(), ((List<Datamart>) promise.future().result()).get(0).getMnemonic());
        assertEquals(datamarts.get(0).getDatamartTables(), ((List<Datamart>) promise.future().result()).get(0).getDatamartTables());
    }

    @Test
    void createSchemaWithServiceError() {
        Promise promise = Promise.promise();
        doAnswer(invocation -> {
            final Handler<AsyncResult<Map<DatamartSchemaKey, DatamartTable>>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("Ошибка создания схемы!")));
            return null;
        }).when(logicalSchemaService).createSchema(any(), any());

        logicalSchemaProvider.getSchema(queryRequest, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }
}