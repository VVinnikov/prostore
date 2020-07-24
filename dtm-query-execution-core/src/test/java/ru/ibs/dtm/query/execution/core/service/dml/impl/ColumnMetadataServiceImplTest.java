package ru.ibs.dtm.query.execution.core.service.dml.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.vertx.core.Vertx;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.junit5.VertxTestContext;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QuerySourceRequest;
import ru.ibs.dtm.query.calcite.core.service.QueryParserService;
import ru.ibs.dtm.query.execution.core.calcite.CoreCalciteContextProvider;
import ru.ibs.dtm.query.execution.core.calcite.CoreCalciteDMLQueryParserService;
import ru.ibs.dtm.query.execution.core.calcite.CoreCalciteSchemaFactory;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.factory.impl.CoreSchemaFactory;
import ru.ibs.dtm.query.execution.core.service.dml.ColumnMetadataService;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;
import ru.ibs.dtm.query.execution.model.metadata.ColumnType;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class ColumnMetadataServiceImplTest {
    private final CalciteConfiguration config = new CalciteConfiguration();
    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final SqlParser.Config configParser = calciteConfiguration.configEddlParser(calciteConfiguration.getSqlParserFactory());
    private final CoreCalciteSchemaFactory calciteSchemaFactory = new CoreCalciteSchemaFactory(new CoreSchemaFactory());
    private final CoreCalciteContextProvider calciteContextProvider = new CoreCalciteContextProvider(configParser, calciteSchemaFactory);
    private final QueryParserService parserService = new CoreCalciteDMLQueryParserService(calciteContextProvider, Vertx.vertx());
    private final ColumnMetadataService service = new ColumnMetadataServiceImpl(parserService);

    @Test
    void getColumnMetadata() throws JsonProcessingException, InterruptedException {
        val testContext = new VertxTestContext();
        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql("select * from dml.accounts");
        sourceRequest.setQueryRequest(queryRequest);
        val datamarts = DatabindCodec.mapper()
                .readValue(loadTextFromFile("schema/dml.json"), new TypeReference<List<Datamart>>() {
                });
        sourceRequest.setLogicalSchema(datamarts);
        List<ColumnMetadata> expectedColumns = Arrays.asList(
                new ColumnMetadata("account_type", ColumnType.VARCHAR),
                new ColumnMetadata("account_id", ColumnType.BIGINT));
        service.getColumnMetadata(sourceRequest, ar -> {
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

    @SneakyThrows
    String loadTextFromFile(String path) {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(path)) {
            assert inputStream != null;
            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
    }
}