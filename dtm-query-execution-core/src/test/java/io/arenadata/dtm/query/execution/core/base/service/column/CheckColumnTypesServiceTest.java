package io.arenadata.dtm.query.execution.core.base.service.column;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.calcite.factory.CoreCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.core.calcite.factory.CoreSchemaFactory;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteContextProvider;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.core.utils.TestUtils;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.Vertx;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.arenadata.dtm.query.execution.core.utils.TestUtils.loadTextFromFile;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckColumnTypesServiceTest {
    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final SqlParser.Config configParser = calciteConfiguration.configEddlParser(calciteConfiguration.getSqlParserFactory());
    private final CoreCalciteSchemaFactory calciteSchemaFactory = new CoreCalciteSchemaFactory(new CoreSchemaFactory());
    private final CoreCalciteContextProvider calciteContextProvider = new CoreCalciteContextProvider(configParser, calciteSchemaFactory);
    private final QueryParserService parserService = new CoreCalciteDMLQueryParserService(calciteContextProvider, Vertx.vertx());
    private final CheckColumnTypesService service = new CheckColumnTypesServiceImpl(parserService);

    @Test
    void checkColumnTypesSuccess() throws JsonProcessingException, InterruptedException {
        val testContext = new VertxTestContext();
        val sql = "select * from dml.accounts";
        val sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        val datamarts = DatabindCodec.mapper()
                .readValue(loadTextFromFile("schema/dml_all_types.json"), new TypeReference<List<Datamart>>() {
                });
        val destColumns = Arrays.asList(
                createEntityField(0, "id", ColumnType.INT, null, 1, 1),
                createEntityField(1, "double_col", ColumnType.DOUBLE, null, null, null),
                createEntityField(2, "float_col", ColumnType.FLOAT, null, null, null),
                createEntityField(3, "varchar_col", ColumnType.VARCHAR, 36, null, null),
                createEntityField(4, "boolean_col", ColumnType.BOOLEAN, null, null, null),
                createEntityField(5, "int_col", ColumnType.INT, null, null, null),
                createEntityField(6, "bigint_col", ColumnType.BIGINT, null, null, null),
                createEntityField(9, "date_col", ColumnType.DATE, null, null, null),
                createEntityField(7, "timestamp_col", ColumnType.TIMESTAMP, 6, null, null),
                createEntityField(8, "time_col", ColumnType.TIME, 5, null, null),
                createEntityField(9, "uuid_col", ColumnType.UUID, 36, null, null),
                createEntityField(9, "char_col", ColumnType.CHAR, 10, null, null),
                createEntityField(9, "int32_col", ColumnType.INT32, null, null, null),
                createEntityField(9, "link_col", ColumnType.LINK, null, null, null)
        );

        service.check(destColumns, new QueryParserRequest(sqlNode, datamarts))
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        assertTrue(ar.result());
                        testContext.completeNow();
                    } else {
                        testContext.failNow(ar.cause());
                    }
                });
        assertThat(testContext.awaitCompletion(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void checkColumnTypesWithConstSuccess() throws JsonProcessingException, InterruptedException {
        val testContext = new VertxTestContext();
        val sql = "select *, 0 as sys_op from dml.accounts";
        val sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        val datamarts = DatabindCodec.mapper()
                .readValue(loadTextFromFile("schema/dml_all_types.json"), new TypeReference<List<Datamart>>() {
                });
        val destColumns = Arrays.asList(
                createEntityField(0, "id", ColumnType.INT, null, 1, 1),
                createEntityField(1, "double_col", ColumnType.DOUBLE, null, null, null),
                createEntityField(2, "float_col", ColumnType.FLOAT, null, null, null),
                createEntityField(3, "varchar_col", ColumnType.VARCHAR, 36, null, null),
                createEntityField(4, "boolean_col", ColumnType.BOOLEAN, null, null, null),
                createEntityField(5, "int_col", ColumnType.INT, null, null, null),
                createEntityField(6, "bigint_col", ColumnType.BIGINT, null, null, null),
                createEntityField(9, "date_col", ColumnType.DATE, null, null, null),
                createEntityField(7, "timestamp_col", ColumnType.TIMESTAMP, 6, null, null),
                createEntityField(8, "time_col", ColumnType.TIME, 5, null, null),
                createEntityField(9, "uuid_col", ColumnType.UUID, 36, null, null),
                createEntityField(9, "char_col", ColumnType.CHAR, 10, null, null),
                createEntityField(9, "int32_col", ColumnType.INT32, null, null, null),
                createEntityField(9, "link_col", ColumnType.LINK, null, null, null),
                createEntityField(9, "sys_op", ColumnType.INT32, null, null, null)
        );

        service.check(destColumns, new QueryParserRequest(sqlNode, datamarts))
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        assertTrue(ar.result());
                        testContext.completeNow();
                    } else {
                        testContext.failNow(ar.cause());
                    }
                });
        assertThat(testContext.awaitCompletion(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void checkColumnTypesFail() throws JsonProcessingException, InterruptedException {
        val testContext = new VertxTestContext();
        val sql = "select * from dml.accounts";
        val sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        val datamarts = DatabindCodec.mapper()
                .readValue(loadTextFromFile("schema/dml_all_types.json"), new TypeReference<List<Datamart>>() {
                });
        val destColumns = Arrays.asList(
                createEntityField(0, "id", ColumnType.INT, null, 1, 1),
                createEntityField(1, "double_col", ColumnType.DOUBLE, null, null, null),
                createEntityField(2, "float_col", ColumnType.FLOAT, null, null, null),
                createEntityField(3, "varchar_col", ColumnType.VARCHAR, 36, null, null),
                createEntityField(4, "boolean_col", ColumnType.BOOLEAN, null, null, null),
                createEntityField(5, "int_col", ColumnType.INT, null, null, null),
                createEntityField(6, "bigint_col", ColumnType.BIGINT, null, null, null),
                createEntityField(9, "date_col", ColumnType.DATE, null, null, null),
                createEntityField(7, "timestamp_col", ColumnType.TIMESTAMP, 6, null, null),
                createEntityField(8, "time_col", ColumnType.TIME, 5, null, null),
                createEntityField(9, "uuid_col", ColumnType.UUID, 36, null, null),
                createEntityField(9, "char_col", ColumnType.CHAR, 10, null, null),
                createEntityField(9, "int32_col", ColumnType.INT32, null, null, null)
        );

        service.check(destColumns, new QueryParserRequest(sqlNode, datamarts))
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        assertFalse(ar.result());
                        testContext.completeNow();
                    } else {
                        testContext.failNow(ar.cause());
                    }
                });
        assertThat(testContext.awaitCompletion(10, TimeUnit.SECONDS)).isTrue();
    }

    EntityField createEntityField(int ordinalPosition, String name, ColumnType type, Integer size,
                                  Integer primaryOrder, Integer shardingOrder) {
        return EntityField.builder()
                .ordinalPosition(ordinalPosition)
                .name(name)
                .type(type)
                .size(size)
                .accuracy(null)
                .nullable(true)
                .primaryOrder(primaryOrder)
                .shardingOrder(shardingOrder)
                .defaultValue(null)
                .build();
    }
}
