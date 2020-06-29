package ru.ibs.dtm.query.execution.plugin.adqm.service;

import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.calcite.CalciteContext;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.CalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.CalciteSchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.CalciteConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.impl.SchemaFactoryImpl;
import ru.ibs.dtm.query.execution.plugin.adqm.model.metadata.*;
import ru.ibs.dtm.query.execution.plugin.adqm.service.impl.enrichment.AdqmCalciteDMLQueryParserServiceImpl;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class QueryParserServiceTest {
    private static final Vertx vertx = Vertx.vertx();
    private static final QueryParserService queryParserService = new AdqmCalciteDMLQueryParserServiceImpl(vertx);
    private static final Datamart datamart = testDatamart();
    private static CalciteContext calciteContext;

    private static Datamart testDatamart() {
        Datamart dm = new Datamart();
        dm.setMnemonic("test_datamart");
        DatamartClass test1 = new DatamartClass();
        test1.setMnemonic("test1");
        test1.setClassAttributes(Arrays.asList(
                new ClassAttribute(UUID.randomUUID(), "col1", new TypeMessage(UUID.randomUUID(), ColumnType.STRING)),
                new ClassAttribute(UUID.randomUUID(), "col2", new TypeMessage(UUID.randomUUID(), ColumnType.STRING))
        ));
        DatamartClass test2 = new DatamartClass();
        test2.setMnemonic("test2");
        test2.setClassAttributes(Arrays.asList(
                new ClassAttribute(UUID.randomUUID(), "col3", new TypeMessage(UUID.randomUUID(), ColumnType.STRING)),
                new ClassAttribute(UUID.randomUUID(), "col4", new TypeMessage(UUID.randomUUID(), ColumnType.STRING))
        ));
        dm.setDatamartClassess(Arrays.asList(test1, test2));
        return dm;
    }

    @BeforeAll
    public static void setup() {
        CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
        calciteConfiguration.init();
        SqlParser.Config parserConfig = calciteConfiguration.configDdlParser(
                calciteConfiguration.ddlParserImplFactory()
        );

        CalciteContextProvider calciteContextProvider = new CalciteContextProvider(
                parserConfig,
                new CalciteSchemaFactory(new SchemaFactoryImpl()));

        calciteContext = calciteContextProvider.context(datamart);
    }

    @SneakyThrows
    @Test
    public void testQueryParse() {
        String query =  "SELECT col1, col2\n" +
                "FROM test_datamart.test1 FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' t\n" +
                "JOIN test_datamart.test2 FOR SYSTEM_TIME AS OF '2020-06-10 23:59:59'\n" +
                "ON t.col1 = test2.col3\n" +
                "WHERE t.Col1 = 1 OR t.Col1 = 10";

        QueryRequest request = new QueryRequest();
        request.setSql(query);
        request.setDatamartMnemonic("test_datamart");

        queryParserService.parse(request, calciteContext, ar -> {
            log.debug("Status: {}, relation: {}", ar.succeeded(), ar.result());
            assertTrue(ar.succeeded());
            assertNotNull(ar.result());
        });

        Thread.sleep(5000L);
        vertx.close();
    }
}