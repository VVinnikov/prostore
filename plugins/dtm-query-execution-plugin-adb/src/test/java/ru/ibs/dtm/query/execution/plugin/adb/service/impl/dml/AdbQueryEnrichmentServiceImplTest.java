package ru.ibs.dtm.query.execution.plugin.adb.service.impl.dml;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.service.DeltaService;
import ru.ibs.dtm.query.calcite.core.service.QueryParserService;
import ru.ibs.dtm.query.execution.plugin.adb.calcite.AdbCalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adb.calcite.AdbCalciteSchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adb.configuration.CalciteConfiguration;
import ru.ibs.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adb.factory.impl.AdbSchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.adb.service.QueryExtendService;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.enrichment.*;
import ru.ibs.dtm.query.execution.plugin.api.request.LlrRequest;
import utils.JsonUtils;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@Slf4j
public class AdbQueryEnrichmentServiceImplTest {

    QueryEnrichmentService adbQueryEnrichmentService;

    public AdbQueryEnrichmentServiceImplTest() {
        QueryExtendService queryExtender = new AdbCalciteDMLQueryExtendServiceImpl();

        CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
        calciteConfiguration.init();
        SqlParser.Config parserConfig = calciteConfiguration.configDdlParser(
                calciteConfiguration.ddlParserImplFactory()
        );

        AdbCalciteContextProvider contextProvider = new AdbCalciteContextProvider(
                parserConfig,
                new AdbCalciteSchemaFactory(new AdbSchemaFactory()));

        AdbQueryGeneratorImpl adbQueryGeneratorimpl = new AdbQueryGeneratorImpl(queryExtender);
        QueryParserService queryParserService = new AdbCalciteDMLQueryParserService(contextProvider, Vertx.vertx());
        adbQueryEnrichmentService = new AdbQueryEnrichmentServiceImpl(
                queryParserService,
                adbQueryGeneratorimpl,
                contextProvider,
                new AdbSchemaExtenderImpl());

        AsyncResult<Long> asyncResultDelta = mock(AsyncResult.class);
        when(asyncResultDelta.succeeded()).thenReturn(true);
        when(asyncResultDelta.result()).thenReturn(1L);
        DeltaService deltaService = mock(DeltaService.class);
        doAnswer((Answer<AsyncResult<Long>>) arg0 -> {
            ((Handler<AsyncResult<Long>>) arg0.getArgument(1)).handle(asyncResultDelta);
            return null;
        }).when(deltaService).getDeltaOnDateTime(any(), any());

    }

    @Test
    void enrich() {
        List<String> result = new ArrayList<>();
        EnrichQueryRequest enrichQueryRequest =
                prepareRequest("select * from test_datamart.pso FOR SYSTEM_TIME AS OF TIMESTAMP '1999-01-08 04:05:06'");
        TestSuite suite = TestSuite.create("the_test_suite");
        suite.test("executeQuery", context -> {
            Async async = context.async();
            adbQueryEnrichmentService.enrich(enrichQueryRequest, ar -> {
                log.debug(ar.toString());
                result.add("OK");
                async.complete();
            });

            async.awaitSuccess(7000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        log.info(result.get(0));
    }

    @Test
    void enrichWithQuotes() {
        String expectedResult = "select id as id from" +
                " (select id, lst_nam, fst_nam, mid_nam, otr_nam, brd, gnr," +
                " snils, inn, tsd, non_rsd_id, phy, r_org, prd_tmz, prd_lan," +
                " r_ctz_shp, r_cty, stu, bss, brd_plc, index" +
                " from test_datamart.pso_history" +
                " where sys_from <= 1 and sys_to >= 1" +
                " union all" +
                " select id, lst_nam, fst_nam, mid_nam, otr_nam, brd, gnr," +
                " snils, inn, tsd, non_rsd_id, phy, r_org, prd_tmz, prd_lan," +
                " r_ctz_shp, r_cty, stu, bss, brd_plc, index" +
                " from test_datamart.pso_actual" +
                " where sys_from <= 1)";
        EnrichQueryRequest enrichQueryRequest = prepareRequest("select \"id\" from \"test_datamart\".\"pso\"");
        String[] result = {""};

        TestSuite suite = TestSuite.create("the_test_suite");
        suite.test("executeQuery", context -> {
            Async async = context.async();
            adbQueryEnrichmentService.enrich(enrichQueryRequest, ar -> {
                if (ar.succeeded()) {
                    result[0] = ar.result();
                }
                async.complete();
            });
            async.awaitSuccess(10000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertTrue(result[0].toLowerCase().contains(expectedResult.toLowerCase()));
    }

    private EnrichQueryRequest prepareRequest(String sql) {
        JsonObject test_datamart = JsonUtils.init("meta_data.json", "TEST_DATAMART");
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(sql);
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic("TEST_DATAMART");
        LlrRequest llrRequest = new LlrRequest(queryRequest, test_datamart);
        return EnrichQueryRequest.generate(llrRequest.getQueryRequest(), llrRequest.getSchema());
    }
}
