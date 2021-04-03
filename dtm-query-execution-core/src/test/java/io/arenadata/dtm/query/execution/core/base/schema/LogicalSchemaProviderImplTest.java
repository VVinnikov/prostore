package io.arenadata.dtm.query.execution.core.base.schema;

import io.arenadata.dtm.common.dto.schema.DatamartSchemaKey;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaService;
import io.arenadata.dtm.query.execution.core.base.service.metadata.impl.LogicalSchemaProviderImpl;
import io.arenadata.dtm.query.execution.core.base.service.metadata.impl.LogicalSchemaServiceImpl;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

class LogicalSchemaProviderImplTest {

    private static final String DATAMART = "test_datamart";

    private final CalciteConfiguration config = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
    private final FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
    private final Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
    private final LogicalSchemaService logicalSchemaService = mock(LogicalSchemaServiceImpl.class);
    private LogicalSchemaProvider logicalSchemaProvider;
    private SqlNode query;

    @BeforeEach
    void setUp() {
        logicalSchemaProvider = new LogicalSchemaProviderImpl(logicalSchemaService);
    }

    @Test
    void createSchemaSuccess() throws SqlParseException {
        query = planner.parse("CREATE SCHEMA " + DATAMART);
        Promise<List<Datamart>> promise = Promise.promise();
        final Map<DatamartSchemaKey, Entity> datamartTableMap = new HashMap<>();
        Entity table1 = Entity.builder()
                .schema(DATAMART)
                .name("pso")
                .build();

        EntityField attr = EntityField.builder()
                .name("id")
                .type(ColumnType.VARCHAR)
                .primaryOrder(1)
                .shardingOrder(1)
                .build();

        table1.setFields(Collections.singletonList(attr));

        EntityField attr2 = attr.toBuilder()
                .size(10)
                .build();

        Entity table2 = table1.toBuilder()
                .name("doc")
                .fields(Collections.singletonList(attr2))
                .build();

        datamartTableMap.put(new DatamartSchemaKey(DATAMART, "doc"), table1);
        datamartTableMap.put(new DatamartSchemaKey(DATAMART, "pso"), table2);

        List<Datamart> datamarts = new ArrayList<>();
        Datamart dm = new Datamart();
        dm.setMnemonic(DATAMART);
        dm.setEntities(Arrays.asList(table2, table1));
        datamarts.add(dm);
        when(logicalSchemaService.createSchemaFromQuery(any()))
                .thenReturn(Future.succeededFuture(datamartTableMap));

        logicalSchemaProvider.getSchemaFromQuery(query, DATAMART)
                .onComplete(promise);
        assertEquals(datamarts.get(0).getMnemonic(), promise.future().result().get(0).getMnemonic());
        assertEquals(datamarts.get(0).getEntities(), promise.future().result().get(0).getEntities());
    }

    @Test
    void createSchemaWithServiceError() {
        Promise<List<Datamart>> promise = Promise.promise();
        when(logicalSchemaService.createSchemaFromQuery(any()))
                .thenReturn(Future.failedFuture(new DtmException("Ошибка создания схемы!")));

        logicalSchemaProvider.getSchemaFromQuery(query, DATAMART)
                .onComplete(promise);
        assertNotNull(promise.future().cause());
    }
}
