package ru.ibs.dtm.query.execution.core.service.dml.impl;

import io.vertx.core.Promise;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QuerySourceRequest;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import ru.ibs.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import ru.ibs.dtm.query.execution.core.service.schema.LogicalSchemaProviderImpl;
import ru.ibs.dtm.query.execution.core.service.schema.LogicalSchemaService;
import ru.ibs.dtm.query.execution.core.service.impl.CoreCalciteDefinitionService;
import ru.ibs.dtm.query.execution.core.service.schema.LogicalSchemaServiceImpl;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class LogicalSchemaProviderImplTest {

    private final CalciteConfiguration config = new CalciteConfiguration();
    private final DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(config.eddlParserImplFactory()));
    private LogicalSchemaProvider logicalSchemaProvider;
    private LogicalSchemaService logicalSchemaService = mock(LogicalSchemaServiceImpl.class);
    private QueryRequest queryRequest;
    private QuerySourceRequest sourceRequest;

    @BeforeEach
    void setUp() {
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test_datamart");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        queryRequest.setSubRequestId("6efad624-b9da-4ba1-9fed-f2da478b08e8");
        sourceRequest = new QuerySourceRequest();
        sourceRequest.setQueryRequest(queryRequest);
        sourceRequest.setSourceType(SourceType.ADB);
    }

    @Test
    void createSchema() {
        Promise promise = Promise.promise();
        val testContext = new VertxTestContext();
        logicalSchemaProvider = new LogicalSchemaProviderImpl(logicalSchemaService, definitionService);
        queryRequest.setSql("select t1.id, cast(t2.id as varchar(10)) as tt from test_datamart.pso t1 \n" +
                "    join test_datamart.doc t2 on t1.id = t2.id\n" +
                "    join test_datamart.obj t3 on t3.id = t1.id\n" +
                "    join test_datamart.reg_cxt t4 on t4.id = t1.id\n" +
                "where t1.id in (1,2,3,4,5,6)");
        logicalSchemaProvider.getSchema(queryRequest, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().result());

    }
}