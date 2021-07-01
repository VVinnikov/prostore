package io.arenadata.dtm.query.execution.plugin.adb.synchronize.executors.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.factory.AdbCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.factory.AdbSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.service.AdbCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.service.AdbCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.service.AdbCalciteDefinitionService;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.QueryExtendService;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.impl.AdbDmlQueryExtendWithoutHistoryService;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.impl.AdbQueryEnrichmentServiceImpl;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.impl.AdbQueryGeneratorImpl;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.impl.AdbSchemaExtenderImpl;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.synchronize.factory.SynchronizeSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.synchronize.factory.impl.AdgSynchronizeSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.synchronize.service.PrepareQueriesOfChangesService;
import io.arenadata.dtm.query.execution.plugin.adb.synchronize.service.impl.PrepareQueriesOfChangesServiceImpl;
import io.arenadata.dtm.query.execution.plugin.api.service.shared.adg.AdgSharedService;
import io.arenadata.dtm.query.execution.plugin.api.shared.adg.AdgSharedProperties;
import io.arenadata.dtm.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class AdgSynchronizeDestinationExecutorComplexTest {
    private static final String ENV = "env";
    private static final String DATAMART = "datamart1";
    private static final Long DELTA_NUM = 1L;
    private static final Long DELTA_NUM_CN_TO = 2L;

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final QueryExtendService queryExtender = new AdbDmlQueryExtendWithoutHistoryService();
    private final AdbCalciteContextProvider contextProvider = new AdbCalciteContextProvider(
            calciteConfiguration.configDdlParser(
                    calciteConfiguration.ddlParserImplFactory()
            ),
            new AdbCalciteSchemaFactory(new AdbSchemaFactory()));
    private final SqlDialect sqlDialect = calciteConfiguration.adbSqlDialect();
    private final DefinitionService<SqlNode> definitionService = new AdbCalciteDefinitionService(calciteConfiguration.configDdlParser(calciteConfiguration.ddlParserImplFactory()));

    @Mock
    private DatabaseExecutor databaseExecutor;

    @Mock
    private AdgSharedService adgSharedService;

    private AdbCalciteDMLQueryParserService parserService;
    private AdbQueryEnrichmentServiceImpl queryEnrichmentService;
    private PrepareQueriesOfChangesService prepareQueriesOfChangesService;
    private SynchronizeSqlFactory synchronizeSqlFactory;
    private AdgSynchronizeDestinationExecutor adgSynchronizeDestinationExecutor;

    @Captor
    private ArgumentCaptor<String> stringArgumentCaptor;

    @BeforeEach
    void setUp(Vertx vertx) {
        parserService = new AdbCalciteDMLQueryParserService(contextProvider, vertx);
        queryEnrichmentService = new AdbQueryEnrichmentServiceImpl(parserService, new AdbQueryGeneratorImpl(queryExtender, sqlDialect), contextProvider, new AdbSchemaExtenderImpl());
        prepareQueriesOfChangesService = new PrepareQueriesOfChangesServiceImpl(parserService, sqlDialect, queryEnrichmentService);
        synchronizeSqlFactory = new AdgSynchronizeSqlFactory(adgSharedService);
        adgSynchronizeDestinationExecutor = new AdgSynchronizeDestinationExecutor(prepareQueriesOfChangesService, databaseExecutor, synchronizeSqlFactory, adgSharedService);

        lenient().when(databaseExecutor.execute(anyString())).thenReturn(Future.succeededFuture(Collections.emptyList()));
        lenient().when(adgSharedService.prepareStaging(any())).thenReturn(Future.succeededFuture());
        lenient().when(adgSharedService.transferData(any())).thenReturn(Future.succeededFuture());
        lenient().when(adgSharedService.getSharedProperties()).thenReturn(new AdgSharedProperties("tarantool:1234", "user", "pass", 3001L, 3002L, 3003L));
    }

    @Test
    void shouldCorrectlySynchronizeWhenOneTable(VertxTestContext testContext) {
        // arrange
        UUID uuid = UUID.randomUUID();

        String viewQuery = "SELECT * from datamart1.tbl1";
        Datamart datamart = prepareDatamart(viewQuery);
        Entity matView = datamart.getEntities().get(0);
        List<Datamart> datamarts = Arrays.asList(datamart);
        SqlNode sqlNode = definitionService.processingQuery(viewQuery);

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, matView, sqlNode, DELTA_NUM, DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adgSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.failed()) {
                testContext.failNow(ar.cause());
                return;
            }

            testContext.verify(() -> {
                verify(databaseExecutor, times(5)).execute(stringArgumentCaptor.capture());
                List<String> allInvocations = stringArgumentCaptor.getAllValues();
                assertThat(allInvocations.get(0))
                        .isEqualToIgnoringNewLines("DROP EXTERNAL TABLE IF EXISTS datamart1.TARANTOOL_EXT_matview");
                assertThat(allInvocations.get(1))
                        .isEqualToIgnoringNewLines("CREATE WRITABLE EXTERNAL TABLE datamart1.TARANTOOL_EXT_matview\n" +
                                "(sys_op int8,sys_to int8,sys_from int8,sys_op int8,sys_to int8,sys_from int8,col_varchar varchar,col_char varchar,col_bigint int8,col_int int8,col_int32 int4,col_double float8,col_float float4,col_date date,col_time time(6),col_timestamp timestamp(6),col_boolean bool,col_uuid varchar(36),col_link varchar,sys_op int8,bucket_id int8) LOCATION ('pxf://env__datamart1__matview_staging?PROFILE=tarantool-upsert&TARANTOOL_SERVER=tarantool:1234&USER=user&PASSWORD=pass&TIMEOUT_CONNECT=3001&TIMEOUT_READ=3002&TIMEOUT_REQUEST=3003')\n" +
                                "FORMAT 'CUSTOM' (FORMATTER = 'pxfwritable_export')");
                assertThat(allInvocations.get(2))
                        .isEqualToIgnoringNewLines("INSERT INTO datamart1.TARANTOOL_EXT_matview SELECT col_varchar, col_char, col_bigint, col_int, col_int32, col_double, col_float, (col_date - DATE '1970-01-01') AS EXPR$7, EXTRACT(EPOCH FROM col_time) * 1000000 AS EXPR$8, EXTRACT(EPOCH FROM col_timestamp) * 1000000 AS EXPR$9, col_boolean, col_uuid, col_link, 1 AS EXPR$13 FROM datamart1.tbl1_actual WHERE COALESCE(sys_to, 9223372036854775807) >= 0 AND (COALESCE(sys_to, 9223372036854775807) <= 0 AND sys_op = 1)");
                assertThat(allInvocations.get(3))
                        .isEqualToIgnoringNewLines("INSERT INTO datamart1.TARANTOOL_EXT_matview SELECT col_varchar, col_char, col_bigint, col_int, col_int32, col_double, col_float, (col_date - DATE '1970-01-01') AS EXPR$7, EXTRACT(EPOCH FROM col_time) * 1000000 AS EXPR$8, EXTRACT(EPOCH FROM col_timestamp) * 1000000 AS EXPR$9, col_boolean, col_uuid, col_link, 0 AS EXPR$13 FROM datamart1.tbl1_actual WHERE sys_from >= 1 AND sys_from <= 1");
                assertThat(allInvocations.get(4))
                        .isEqualToIgnoringNewLines("DROP EXTERNAL TABLE IF EXISTS datamart1.TARANTOOL_EXT_matview");
                verifyNoMoreInteractions(databaseExecutor);
                assertEquals(DELTA_NUM, ar.result());
            }).completeNow();
        });
    }

    @Test
    void shouldCorrectlySynchronizeWhenMultipleTables(VertxTestContext testContext) {
        // arrange
        UUID uuid = UUID.randomUUID();
        String viewQuery = "SELECT * from datamart1.tbl1 join datamart1.tbl2 on tbl1.col_bigint = tbl2.col_bigint";
        SqlNode sqlNode = definitionService.processingQuery(viewQuery);
        Datamart datamart = prepareDatamart(viewQuery);
        Entity matView = datamart.getEntities().get(0);
        List<Datamart> datamarts = Arrays.asList(datamart);

        SynchronizeRequest synchronizeRequest = new SynchronizeRequest(uuid, ENV, DATAMART, datamarts, matView, sqlNode, DELTA_NUM, DELTA_NUM_CN_TO);

        // act
        Future<Long> result = adgSynchronizeDestinationExecutor.execute(synchronizeRequest);

        // assert
        result.onComplete(ar -> {
            if (ar.failed()) {
                testContext.failNow(ar.cause());
                return;
            }

            testContext.verify(() -> {
                verify(databaseExecutor, times(5)).execute(stringArgumentCaptor.capture());
                List<String> allInvocations = stringArgumentCaptor.getAllValues();
                assertThat(allInvocations.get(0))
                        .isEqualToIgnoringNewLines("DROP EXTERNAL TABLE IF EXISTS datamart1.TARANTOOL_EXT_matview");
                assertThat(allInvocations.get(1))
                        .isEqualToIgnoringNewLines("CREATE WRITABLE EXTERNAL TABLE datamart1.TARANTOOL_EXT_matview\n" +
                                "(sys_op int8,sys_to int8,sys_from int8,sys_op int8,sys_to int8,sys_from int8,sys_op int8,sys_to int8,sys_from int8,sys_op int8,sys_to int8,sys_from int8,col_varchar varchar,col_char varchar,col_bigint int8,col_int int8,col_int32 int4,col_double float8,col_float float4,col_date date,col_time time(6),col_timestamp timestamp(6),col_boolean bool,col_uuid varchar(36),col_link varchar,sys_op int8,bucket_id int8) LOCATION ('pxf://env__datamart1__matview_staging?PROFILE=tarantool-upsert&TARANTOOL_SERVER=tarantool:1234&USER=user&PASSWORD=pass&TIMEOUT_CONNECT=3001&TIMEOUT_READ=3002&TIMEOUT_REQUEST=3003')\n" +
                                "FORMAT 'CUSTOM' (FORMATTER = 'pxfwritable_export')");
                assertThat(allInvocations.get(2))
                        .isEqualToIgnoringNewLines("INSERT INTO datamart1.TARANTOOL_EXT_matview SELECT t0.col_varchar, t0.col_char, t0.col_bigint, t0.col_int, t0.col_int32, t0.col_double, t0.col_float, (t0.col_date - DATE '1970-01-01') AS EXPR$7, EXTRACT(EPOCH FROM t0.col_time) * 1000000 AS EXPR$8, EXTRACT(EPOCH FROM t0.col_timestamp) * 1000000 AS EXPR$9, t0.col_boolean, t0.col_uuid, t0.col_link, t2.col_varchar AS col_varchar0, t2.col_char AS col_char0, t2.col_bigint AS col_bigint0, t2.col_int AS col_int0, t2.col_int32 AS col_int320, t2.col_double AS col_double0, t2.col_float AS col_float0, (t2.col_date - DATE '1970-01-01') AS col_date0, EXTRACT(EPOCH FROM t2.col_time) * 1000000 AS col_time0, EXTRACT(EPOCH FROM t2.col_timestamp) * 1000000 AS col_timestamp0, t2.col_boolean AS col_boolean0, t2.col_uuid AS col_uuid0, t2.col_link AS col_link0, 1 AS EXPR$26 FROM (SELECT col_varchar, col_char, col_bigint, col_int, col_int32, col_double, col_float, col_date, col_time, col_timestamp, col_boolean, col_uuid, col_link FROM datamart1.tbl1_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0) AS t0 INNER JOIN (SELECT col_varchar, col_char, col_bigint, col_int, col_int32, col_double, col_float, col_date, col_time, col_timestamp, col_boolean, col_uuid, col_link FROM datamart1.tbl2_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0) AS t2 ON t0.col_bigint = t2.col_bigint EXCEPT SELECT t0.col_varchar, t0.col_char, t0.col_bigint, t0.col_int, t0.col_int32, t0.col_double, t0.col_float, (t0.col_date - DATE '1970-01-01') AS EXPR$7, EXTRACT(EPOCH FROM t0.col_time) * 1000000 AS EXPR$8, EXTRACT(EPOCH FROM t0.col_timestamp) * 1000000 AS EXPR$9, t0.col_boolean, t0.col_uuid, t0.col_link, t2.col_varchar AS col_varchar0, t2.col_char AS col_char0, t2.col_bigint AS col_bigint0, t2.col_int AS col_int0, t2.col_int32 AS col_int320, t2.col_double AS col_double0, t2.col_float AS col_float0, (t2.col_date - DATE '1970-01-01') AS col_date0, EXTRACT(EPOCH FROM t2.col_time) * 1000000 AS col_time0, EXTRACT(EPOCH FROM t2.col_timestamp) * 1000000 AS col_timestamp0, t2.col_boolean AS col_boolean0, t2.col_uuid AS col_uuid0, t2.col_link AS col_link0, 1 AS EXPR$26 FROM (SELECT col_varchar, col_char, col_bigint, col_int, col_int32, col_double, col_float, col_date, col_time, col_timestamp, col_boolean, col_uuid, col_link FROM datamart1.tbl1_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t0 INNER JOIN (SELECT col_varchar, col_char, col_bigint, col_int, col_int32, col_double, col_float, col_date, col_time, col_timestamp, col_boolean, col_uuid, col_link FROM datamart1.tbl2_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t2 ON t0.col_bigint = t2.col_bigint");
                assertThat(allInvocations.get(3))
                        .isEqualToIgnoringNewLines("INSERT INTO datamart1.TARANTOOL_EXT_matview SELECT t0.col_varchar, t0.col_char, t0.col_bigint, t0.col_int, t0.col_int32, t0.col_double, t0.col_float, (t0.col_date - DATE '1970-01-01') AS EXPR$7, EXTRACT(EPOCH FROM t0.col_time) * 1000000 AS EXPR$8, EXTRACT(EPOCH FROM t0.col_timestamp) * 1000000 AS EXPR$9, t0.col_boolean, t0.col_uuid, t0.col_link, t2.col_varchar AS col_varchar0, t2.col_char AS col_char0, t2.col_bigint AS col_bigint0, t2.col_int AS col_int0, t2.col_int32 AS col_int320, t2.col_double AS col_double0, t2.col_float AS col_float0, (t2.col_date - DATE '1970-01-01') AS col_date0, EXTRACT(EPOCH FROM t2.col_time) * 1000000 AS col_time0, EXTRACT(EPOCH FROM t2.col_timestamp) * 1000000 AS col_timestamp0, t2.col_boolean AS col_boolean0, t2.col_uuid AS col_uuid0, t2.col_link AS col_link0, 0 AS EXPR$26 FROM (SELECT col_varchar, col_char, col_bigint, col_int, col_int32, col_double, col_float, col_date, col_time, col_timestamp, col_boolean, col_uuid, col_link FROM datamart1.tbl1_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t0 INNER JOIN (SELECT col_varchar, col_char, col_bigint, col_int, col_int32, col_double, col_float, col_date, col_time, col_timestamp, col_boolean, col_uuid, col_link FROM datamart1.tbl2_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t2 ON t0.col_bigint = t2.col_bigint EXCEPT SELECT t0.col_varchar, t0.col_char, t0.col_bigint, t0.col_int, t0.col_int32, t0.col_double, t0.col_float, (t0.col_date - DATE '1970-01-01') AS EXPR$7, EXTRACT(EPOCH FROM t0.col_time) * 1000000 AS EXPR$8, EXTRACT(EPOCH FROM t0.col_timestamp) * 1000000 AS EXPR$9, t0.col_boolean, t0.col_uuid, t0.col_link, t2.col_varchar AS col_varchar0, t2.col_char AS col_char0, t2.col_bigint AS col_bigint0, t2.col_int AS col_int0, t2.col_int32 AS col_int320, t2.col_double AS col_double0, t2.col_float AS col_float0, (t2.col_date - DATE '1970-01-01') AS col_date0, EXTRACT(EPOCH FROM t2.col_time) * 1000000 AS col_time0, EXTRACT(EPOCH FROM t2.col_timestamp) * 1000000 AS col_timestamp0, t2.col_boolean AS col_boolean0, t2.col_uuid AS col_uuid0, t2.col_link AS col_link0, 0 AS EXPR$26 FROM (SELECT col_varchar, col_char, col_bigint, col_int, col_int32, col_double, col_float, col_date, col_time, col_timestamp, col_boolean, col_uuid, col_link FROM datamart1.tbl1_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0) AS t0 INNER JOIN (SELECT col_varchar, col_char, col_bigint, col_int, col_int32, col_double, col_float, col_date, col_time, col_timestamp, col_boolean, col_uuid, col_link FROM datamart1.tbl2_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0) AS t2 ON t0.col_bigint = t2.col_bigint");
                assertThat(allInvocations.get(4))
                        .isEqualToIgnoringNewLines("DROP EXTERNAL TABLE IF EXISTS datamart1.TARANTOOL_EXT_matview");
                verifyNoMoreInteractions(databaseExecutor);
                assertEquals(DELTA_NUM, ar.result());
            }).completeNow();
        });
    }

    private Datamart prepareDatamart(String viewQuery) {
        List<EntityField> fields = new ArrayList<>();
        int pos = 1;
        for (ColumnType columnType : ColumnType.values()) {
            if (columnType == ColumnType.ANY || columnType == ColumnType.BLOB) continue;

            EntityField field = EntityField.builder()
                    .ordinalPosition(pos++)
                    .name("col_" + columnType.name().toLowerCase())
                    .type(columnType)
                    .nullable(true)
                    .build();

            switch (columnType) {
                case TIME:
                case TIMESTAMP:
                    field.setAccuracy(6);
                    break;
                case CHAR:
                case VARCHAR:
                    field.setSize(100);
                    break;
                case UUID:
                    field.setSize(36);
                    break;
            }

            fields.add(field);
        }

        Entity tbl1 = Entity.builder()
                .schema(DATAMART)
                .name("tbl1")
                .fields(fields)
                .entityType(EntityType.TABLE)
                .destination(EnumSet.of(SourceType.ADB))
                .build();

        Entity tbl2 = Entity.builder()
                .schema(DATAMART)
                .name("tbl2")
                .fields(fields)
                .entityType(EntityType.TABLE)
                .destination(EnumSet.of(SourceType.ADB))
                .build();
        Entity matView = Entity.builder()
                .schema(DATAMART)
                .name("matview")
                .fields(fields)
                .viewQuery(viewQuery)
                .entityType(EntityType.MATERIALIZED_VIEW)
                .destination(EnumSet.of(SourceType.ADG))
                .materializedDeltaNum(null)
                .materializedDataSource(SourceType.ADB)
                .build();
        return Datamart.builder()
                .isDefault(true)
                .mnemonic(DATAMART)
                .entities(Arrays.asList(
                        matView, tbl1, tbl2
                ))
                .build();
    }
}