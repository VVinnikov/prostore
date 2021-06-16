package io.arenadata.dtm.query.execution.plugin.adb.synchronize.service.impl;

import io.arenadata.dtm.common.calcite.CalciteContext;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.provider.CalciteContextProvider;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.factory.AdbCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.factory.AdbSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.service.AdbCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.service.AdbCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adb.synchronize.service.PrepareRequestOfChangesRequest;
import io.arenadata.dtm.query.execution.plugin.adb.synchronize.service.PrepareRequestOfChangesResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class PrepareQueriesOfChangesServiceImplTest {

    private static final Datamart DATAMART_1 = Datamart.builder()
            .mnemonic("datamart1")
            .entities(Arrays.asList(
                    Entity.builder()
                            .entityType(EntityType.TABLE)
                            .name("dates")
                            .schema("datamart1")
                            .fields(Arrays.asList(
                                    EntityField.builder()
                                            .name("id")
                                            .type(ColumnType.BIGINT)
                                            .nullable(false)
                                            .primaryOrder(1)
                                            .build(),
                                    EntityField.builder()
                                            .name("col_timestamp")
                                            .type(ColumnType.TIMESTAMP)
                                            .nullable(true)
                                            .build(),
                                    EntityField.builder()
                                            .name("col_time")
                                            .type(ColumnType.TIME)
                                            .nullable(true)
                                            .build(),
                                    EntityField.builder()
                                            .name("col_date")
                                            .type(ColumnType.DATE)
                                            .nullable(true)
                                            .build()
                            )).build(),
                    Entity.builder()
                            .entityType(EntityType.TABLE)
                            .name("surnames")
                            .schema("datamart1")
                            .fields(Arrays.asList(
                                    EntityField.builder()
                                            .name("id")
                                            .type(ColumnType.BIGINT)
                                            .nullable(false)
                                            .primaryOrder(1)
                                            .build(),
                                    EntityField.builder()
                                            .name("surname")
                                            .type(ColumnType.VARCHAR)
                                            .size(100)
                                            .nullable(true)
                                            .build()
                            )).build()
            ))
            .isDefault(true)
            .build();

    private static final Datamart DATAMART_2 = Datamart.builder()
            .mnemonic("datamart2")
            .entities(Collections.singletonList(
                    Entity.builder()
                            .entityType(EntityType.TABLE)
                            .name("names")
                            .schema("datamart2")
                            .fields(Arrays.asList(
                                    EntityField.builder()
                                            .name("id")
                                            .type(ColumnType.BIGINT)
                                            .nullable(false)
                                            .primaryOrder(1)
                                            .build(),
                                    EntityField.builder()
                                            .name("name")
                                            .type(ColumnType.VARCHAR)
                                            .size(100)
                                            .nullable(true)
                                            .build()
                            )).build()
            ))
            .isDefault(false)
            .build();

    private static final List<Datamart> SINGLE_DATAMART = Collections.singletonList(DATAMART_1);
    private static final List<Datamart> TWO_DATAMARTS = Arrays.asList(DATAMART_1, DATAMART_2);

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configDdlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final AdbCalciteSchemaFactory coreSchemaFactory = new AdbCalciteSchemaFactory(new AdbSchemaFactory());
    private final CalciteContextProvider contextProvider = new AdbCalciteContextProvider(parserConfig, coreSchemaFactory);
    private final SqlDialect sqlDialect = calciteConfiguration.adbSqlDialect();

    @Mock
    private QueryEnrichmentService queryEnrichmentService;

    @Captor
    private ArgumentCaptor<EnrichQueryRequest> enrichQueryRequestArgumentCaptor;

    private QueryParserService queryParserService;

    private PrepareQueriesOfChangesServiceImpl queriesOfChangesService;

    @BeforeEach
    void setUp(Vertx vertx) {
        lenient().when(queryEnrichmentService.enrich(any())).thenAnswer(invocationOnMock -> {
            EnrichQueryRequest argument = invocationOnMock.getArgument(0);
            return Future.succeededFuture(argument.getQuery().toSqlString(sqlDialect).toString());
        });
        queryParserService = new AdbCalciteDMLQueryParserService(contextProvider, vertx);
        queriesOfChangesService = new PrepareQueriesOfChangesServiceImpl(queryParserService, sqlDialect, queryEnrichmentService);
    }

    @Test
    void shouldSuccessWhenOneTable(VertxTestContext ctx) {
        // arrange
        SqlNode query = parse("SELECT id, col_timestamp, col_date, col_time FROM datamart1.dates", SINGLE_DATAMART);

        // act
        Future<PrepareRequestOfChangesResult> result = queriesOfChangesService.prepare(new PrepareRequestOfChangesRequest(SINGLE_DATAMART, "dev", 10, query));

        // assert
        String expectedNewQuery = "SELECT dates.id, CAST(EXTRACT(EPOCH FROM dates.col_timestamp) * 1000000 AS BIGINT), (dates.col_date - DATE '1970-01-01'), CAST(EXTRACT(EPOCH FROM dates.col_time) * 1000000 AS BIGINT), 0, NULL\n" +
                "FROM datamart1.dates FOR SYSTEM_TIME STARTED IN (10,10) AS dates";
        String expectedDeletedQuery = "SELECT dates.id, CAST(EXTRACT(EPOCH FROM dates.col_timestamp) * 1000000 AS BIGINT), (dates.col_date - DATE '1970-01-01'), CAST(EXTRACT(EPOCH FROM dates.col_time) * 1000000 AS BIGINT), 1, NULL\n" +
                "FROM datamart1.dates FOR SYSTEM_TIME FINISHED IN (10,10) AS dates";

        result.onComplete(event -> {
            if (event.failed()) {
                ctx.failNow(new AssertionError("Unexpected failure", event.cause()));
                return;
            }

            PrepareRequestOfChangesResult queriesOfChanges = event.result();

            ctx.verify(() -> {
                verify(queryEnrichmentService, times(2)).enrich(enrichQueryRequestArgumentCaptor.capture());
                verifyNoMoreInteractions(queryEnrichmentService);
                List<EnrichQueryRequest> allValues = enrichQueryRequestArgumentCaptor.getAllValues();

                // assert new query
                EnrichQueryRequest firstRequest = allValues.get(0);
                String firstQuery = firstRequest.getQuery().toSqlString(sqlDialect).toString();
                assertThat(expectedNewQuery).isEqualToNormalizingNewlines(firstQuery);

                // assert deleted query
                EnrichQueryRequest secondRequest = allValues.get(1);
                String secondQuery = secondRequest.getQuery().toSqlString(sqlDialect).toString();
                assertThat(expectedDeletedQuery).isEqualToNormalizingNewlines(secondQuery);

                // assert result
                assertThat(expectedNewQuery).isEqualToNormalizingNewlines(queriesOfChanges.getNewRecordsQuery());
                assertThat(expectedDeletedQuery).isEqualToNormalizingNewlines(queriesOfChanges.getDeletedRecordsQuery());
            }).completeNow();
        });
    }

    @Test
    void shouldSuccessWhenMultipleComplexTables(VertxTestContext ctx) {
        // arrange
        SqlNode query = parse("SELECT t0.id, col_timestamp, col_date, col_time, name, surname FROM (SELECT t1.id,col_timestamp,col_date,col_time,name FROM datamart1.dates as t1 JOIN datamart2.names as t2 ON t1.id = t2.id) as t0 JOIN surnames as t3 ON t0.id = t3.id", TWO_DATAMARTS);

        // act
        Future<PrepareRequestOfChangesResult> result = queriesOfChangesService.prepare(new PrepareRequestOfChangesRequest(TWO_DATAMARTS, "dev", 10, query));

        // assert
        String expectedNewFreshQuery = "SELECT t0.id, CAST(EXTRACT(EPOCH FROM t0.col_timestamp) * 1000000 AS BIGINT), (t0.col_date - DATE '1970-01-01'), CAST(EXTRACT(EPOCH FROM t0.col_time) * 1000000 AS BIGINT), t0.name, t3.surname, 0, NULL\n" +
                "FROM (SELECT t1.id, t1.col_timestamp, t1.col_date, t1.col_time, t2.name\n" +
                "FROM datamart1.dates FOR SYSTEM_TIME AS OF DELTA_NUM 10 AS t1\n" +
                "INNER JOIN datamart2.names FOR SYSTEM_TIME AS OF DELTA_NUM 10 AS t2 ON t1.id = t2.id) AS t0\n" +
                "INNER JOIN datamart1.surnames FOR SYSTEM_TIME AS OF DELTA_NUM 10 AS t3 ON t0.id = t3.id";
        String expectedNewStaleQuery = "SELECT t0.id, CAST(EXTRACT(EPOCH FROM t0.col_timestamp) * 1000000 AS BIGINT), (t0.col_date - DATE '1970-01-01'), CAST(EXTRACT(EPOCH FROM t0.col_time) * 1000000 AS BIGINT), t0.name, t3.surname, 0, NULL\n" +
                "FROM (SELECT t1.id, t1.col_timestamp, t1.col_date, t1.col_time, t2.name\n" +
                "FROM datamart1.dates FOR SYSTEM_TIME AS OF DELTA_NUM 9 AS t1\n" +
                "INNER JOIN datamart2.names FOR SYSTEM_TIME AS OF DELTA_NUM 9 AS t2 ON t1.id = t2.id) AS t0\n" +
                "INNER JOIN datamart1.surnames FOR SYSTEM_TIME AS OF DELTA_NUM 9 AS t3 ON t0.id = t3.id";
        String expectedDeletedFreshQuery = "SELECT t0.id, CAST(EXTRACT(EPOCH FROM t0.col_timestamp) * 1000000 AS BIGINT), (t0.col_date - DATE '1970-01-01'), CAST(EXTRACT(EPOCH FROM t0.col_time) * 1000000 AS BIGINT), t0.name, t3.surname, 1, NULL\n" +
                "FROM (SELECT t1.id, t1.col_timestamp, t1.col_date, t1.col_time, t2.name\n" +
                "FROM datamart1.dates FOR SYSTEM_TIME AS OF DELTA_NUM 10 AS t1\n" +
                "INNER JOIN datamart2.names FOR SYSTEM_TIME AS OF DELTA_NUM 10 AS t2 ON t1.id = t2.id) AS t0\n" +
                "INNER JOIN datamart1.surnames FOR SYSTEM_TIME AS OF DELTA_NUM 10 AS t3 ON t0.id = t3.id";
        String expectedDeletedStaleQuery = "SELECT t0.id, CAST(EXTRACT(EPOCH FROM t0.col_timestamp) * 1000000 AS BIGINT), (t0.col_date - DATE '1970-01-01'), CAST(EXTRACT(EPOCH FROM t0.col_time) * 1000000 AS BIGINT), t0.name, t3.surname, 1, NULL\n" +
                "FROM (SELECT t1.id, t1.col_timestamp, t1.col_date, t1.col_time, t2.name\n" +
                "FROM datamart1.dates FOR SYSTEM_TIME AS OF DELTA_NUM 9 AS t1\n" +
                "INNER JOIN datamart2.names FOR SYSTEM_TIME AS OF DELTA_NUM 9 AS t2 ON t1.id = t2.id) AS t0\n" +
                "INNER JOIN datamart1.surnames FOR SYSTEM_TIME AS OF DELTA_NUM 9 AS t3 ON t0.id = t3.id";

        result.onComplete(event -> {
            if (event.failed()) {
                ctx.failNow(new AssertionError("Unexpected failure", event.cause()));
                return;
            }

            PrepareRequestOfChangesResult queriesOfChanges = event.result();

            ctx.verify(() -> {
                verify(queryEnrichmentService, times(4)).enrich(enrichQueryRequestArgumentCaptor.capture());
                verifyNoMoreInteractions(queryEnrichmentService);
                List<EnrichQueryRequest> allValues = enrichQueryRequestArgumentCaptor.getAllValues();

                // assert new fresh query
                EnrichQueryRequest firstFreshRequest = allValues.get(0);
                String firstFreshQuery = firstFreshRequest.getQuery().toSqlString(sqlDialect).toString();
                assertThat(expectedNewFreshQuery).isEqualToNormalizingNewlines(firstFreshQuery);
                // assert new stale query
                EnrichQueryRequest firstStaleRequest = allValues.get(1);
                String firstStaleQuery = firstStaleRequest.getQuery().toSqlString(sqlDialect).toString();
                assertThat(expectedNewStaleQuery).isEqualToNormalizingNewlines(firstStaleQuery);

                // assert deleted fresh query
                EnrichQueryRequest secondStaleRequest = allValues.get(2);
                String secondStaleQuery = secondStaleRequest.getQuery().toSqlString(sqlDialect).toString();
                assertThat(expectedDeletedStaleQuery).isEqualToNormalizingNewlines(secondStaleQuery);

                // assert deleted stale query
                EnrichQueryRequest secondFreshRequest = allValues.get(3);
                String secondFreshQuery = secondFreshRequest.getQuery().toSqlString(sqlDialect).toString();
                assertThat(expectedDeletedFreshQuery).isEqualToNormalizingNewlines(secondFreshQuery);

                assertThat(expectedNewFreshQuery + " EXCEPT " + expectedNewStaleQuery).isEqualToNormalizingNewlines(queriesOfChanges.getNewRecordsQuery());
                assertThat(expectedDeletedStaleQuery + " EXCEPT " + expectedDeletedFreshQuery).isEqualToNormalizingNewlines(queriesOfChanges.getDeletedRecordsQuery());
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenParseFailed(VertxTestContext ctx) {
        // arrange
        SqlNode query = parse("SELECT * FROM dates, datamart2.names", TWO_DATAMARTS);

        // act
        Future<PrepareRequestOfChangesResult> result = queriesOfChangesService.prepare(new PrepareRequestOfChangesRequest(SINGLE_DATAMART, "dev", 10, query));

        // assert
        result.onComplete(event -> {
            if (event.succeeded()) {
                ctx.failNow(new AssertionError("Unexpected success", event.cause()));
                return;
            }

            ctx.verify(() -> {
                Throwable cause = event.cause();
                Assertions.assertSame(DtmException.class, cause.getClass());
                Assertions.assertTrue(cause.getMessage().contains("Object 'datamart2' not found"));
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenNoTables(VertxTestContext ctx) {
        // arrange
        SqlNode query = parse("SELECT 1", TWO_DATAMARTS);

        // act
        Future<PrepareRequestOfChangesResult> result = queriesOfChangesService.prepare(new PrepareRequestOfChangesRequest(SINGLE_DATAMART, "dev", 10, query));

        // assert
        result.onComplete(event -> {
            if (event.succeeded()) {
                ctx.failNow(new AssertionError("Unexpected success", event.cause()));
                return;
            }

            ctx.verify(() -> {
                Throwable cause = event.cause();
                Assertions.assertSame(DtmException.class, cause.getClass());
                Assertions.assertTrue(cause.getMessage().contains("No tables in query"));
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenEnrichmentFailed(VertxTestContext ctx) {
        // arrange
        DtmException expectedException = new DtmException("Enrich exception");
        reset(queryEnrichmentService);
        when(queryEnrichmentService.enrich(any())).thenReturn(Future.failedFuture(expectedException));
        SqlNode query = parse("SELECT id, col_timestamp, col_date, col_time FROM datamart1.dates", SINGLE_DATAMART);

        // act
        Future<PrepareRequestOfChangesResult> result = queriesOfChangesService.prepare(new PrepareRequestOfChangesRequest(SINGLE_DATAMART, "dev", 10, query));

        // assert
        result.onComplete(event -> {
            if (event.succeeded()) {
                ctx.failNow(new AssertionError("Unexpected success", event.cause()));
                return;
            }

            ctx.verify(() -> {
                Throwable cause = event.cause();
                Assertions.assertSame(cause, expectedException);
            }).completeNow();
        });
    }

    private SqlNode parse(String query, List<Datamart> datamarts) {
        try {
            CalciteContext context = contextProvider.context(datamarts);
            SqlNode sqlNode = context.getPlanner().parse(query);
            return context.getPlanner().validate(sqlNode);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}