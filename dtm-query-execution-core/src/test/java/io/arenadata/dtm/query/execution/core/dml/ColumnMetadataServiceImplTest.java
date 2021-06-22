package io.arenadata.dtm.query.execution.core.dml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.calcite.factory.CoreCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.core.calcite.factory.CoreSchemaFactory;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteContextProvider;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.core.dml.service.ColumnMetadataService;
import io.arenadata.dtm.query.execution.core.dml.service.impl.ColumnMetadataServiceImpl;
import io.arenadata.dtm.query.execution.core.utils.TestUtils;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.Vertx;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.arenadata.dtm.query.execution.core.utils.TestUtils.loadTextFromFile;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class ColumnMetadataServiceImplTest {
    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final SqlParser.Config configParser = calciteConfiguration.configEddlParser(calciteConfiguration.getSqlParserFactory());
    private final CoreCalciteSchemaFactory calciteSchemaFactory = new CoreCalciteSchemaFactory(new CoreSchemaFactory());
    private final CoreCalciteContextProvider calciteContextProvider = new CoreCalciteContextProvider(configParser, calciteSchemaFactory);
    private final QueryParserService parserService = new CoreCalciteDMLQueryParserService(calciteContextProvider, Vertx.vertx());
    private final ColumnMetadataService service = new ColumnMetadataServiceImpl(parserService);

    @Test
    void getColumnMetadata() throws JsonProcessingException, InterruptedException {
        val testContext = new VertxTestContext();
        val sql = "select * from dml.accounts";
        val datamarts = DatabindCodec.mapper()
                .readValue(loadTextFromFile("schema/dml_all_types.json"), new TypeReference<List<Datamart>>() {
                });
        List<ColumnMetadata> expectedColumns = Arrays.asList(
                new ColumnMetadata("id", ColumnType.INT),
                new ColumnMetadata("double_col", ColumnType.DOUBLE),
                new ColumnMetadata("float_col", ColumnType.FLOAT),
                new ColumnMetadata("varchar_col", ColumnType.VARCHAR, 36),
                new ColumnMetadata("boolean_col", ColumnType.BOOLEAN),
                new ColumnMetadata("int_col", ColumnType.INT),
                new ColumnMetadata("bigint_col", ColumnType.BIGINT),
                new ColumnMetadata("date_col", ColumnType.DATE),
                new ColumnMetadata("timestamp_col", ColumnType.TIMESTAMP, 6),
                new ColumnMetadata("time_col", ColumnType.TIME, 5),
                new ColumnMetadata("uuid_col", ColumnType.VARCHAR, 36),
                new ColumnMetadata("char_col", ColumnType.CHAR, 10),
                new ColumnMetadata("int32_col", ColumnType.INT32),
                new ColumnMetadata("link_col", ColumnType.LINK));
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        service.getColumnMetadata(new QueryParserRequest(sqlNode, datamarts))
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        log.info("Result columns: {}", ar.result());
                        assertEquals(expectedColumns, ar.result());
                        testContext.completeNow();
                    } else {
                        testContext.failNow(ar.cause());
                    }
                });
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }
}
