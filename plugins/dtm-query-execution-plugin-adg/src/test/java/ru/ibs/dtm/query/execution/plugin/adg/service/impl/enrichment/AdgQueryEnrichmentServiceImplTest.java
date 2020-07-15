package ru.ibs.dtm.query.execution.plugin.adg.service.impl.enrichment;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.dto.ActualDeltaRequest;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.service.DeltaService;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.plugin.adg.calcite.AdgCalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adg.calcite.CalciteSchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.CalciteConfiguration;
import ru.ibs.dtm.query.execution.plugin.adg.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adg.factory.SchemaFactoryImpl;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryExtendService;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryParserService;
import ru.ibs.dtm.query.execution.plugin.adg.service.impl.query.AdgQueryRegexPreprocessor;
import ru.ibs.dtm.query.execution.plugin.adg.utils.JsonUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class AdgQueryEnrichmentServiceImplTest {

    private QueryEnrichmentService enrichService;
    private VertxTestContext testContext = new VertxTestContext();
    private QueryParserService queryParserService;
    private DeltaService deltaService = mock(DeltaService.class);

    public AdgQueryEnrichmentServiceImplTest() {
        CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
        AdgCalciteContextProvider contextProvider = new AdgCalciteContextProvider(
                calciteConfiguration.configDdlParser(calciteConfiguration.ddlParserImplFactory()));
        AdgQueryRegexPreprocessor regexPreprocessor = new AdgQueryRegexPreprocessor();

        queryParserService = new AdgCalciteDmlQueryParserServiceImpl(
                new AdgSchemaExtenderImpl(),
                Vertx.vertx(),
                new CalciteSchemaFactory(
                        new SchemaFactoryImpl()));
        QueryExtendService queryExtendService = new AdgCalciteDmlQueryExtendServiceImpl();

        enrichService = new AdgQueryEnrichmentServiceImpl(
                deltaService,
                regexPreprocessor,
                contextProvider,
                queryParserService,
                new AdgQueryGeneratorImpl(queryExtendService));
    }

    @Test
    @Disabled
    void enrichWithoutTimestamp() throws Throwable {
        //FIXME
        enrich("SELECT id " +
                "FROM test_datamart.reg_cxt FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14'");
    }

    @Test
    @Disabled
    void enrichWithTimestamp() throws Throwable {
        //FIXME
        enrich("SELECT id " +
                "FROM test_datamart.reg_cxt FOR SYSTEM_TIME AS OF TIMESTAMP '2019-12-23 15:15:14'");
    }

    @Test
    @Disabled
    void enrichWithQuotes() throws Throwable {
        //FIXME
        enrich("SELECT \"id\" " +
                "FROM \"test_datamart\".\"reg_cxt\" FOR SYSTEM_TIME AS OF TIMESTAMP '2019-12-23 15:15:14'");
    }

    @Test
    @Disabled
    void enrichWithoutSchema() throws Throwable {
        //FIXME
        enrich("SELECT id " +
                "FROM reg_cxt FOR SYSTEM_TIME AS OF TIMESTAMP '2019-12-23 15:15:14'");
    }

    private void enrich(String sql) throws Throwable {
        class EnrichResultOk {
            private List<ActualDeltaRequest> deltaRequests;
        }
        final EnrichResultOk enrichResultOk = new EnrichResultOk();

        doAnswer(invocation -> {
            enrichResultOk.deltaRequests = invocation.getArgument(0);
            final Handler<AsyncResult<List<Long>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(Arrays.asList(2L, 1L)));
            return null;
        }).when(deltaService).getDeltasOnDateTimes(any(), any());

        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(sql);
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic("test_datamart");
        final JsonObject jsonSchema = JsonUtils.init("meta_data.json", "test_datamart");
        List<Datamart> schema = new ArrayList<>();
        schema.add(jsonSchema.mapTo(Datamart.class));
        String expectedSql = "SELECT * FROM \"reg_cxt_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT * FROM \"reg_cxt_actual\" WHERE \"sys_from\" <= 2";
        String[] sqlResult = {""};

        TestSuite suite = TestSuite.create("the_test_suite");
        suite.test("executeQuery", context -> {
            Async async = context.async();
            enrichService.enrich(EnrichQueryRequest.generate(queryRequest, schema), ar -> {
                if (ar.succeeded()) {
                    sqlResult[0] = ar.result();
                }
                async.complete();
            });
            async.awaitSuccess(10000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertTrue(sqlResult[0].toLowerCase().contains(expectedSql.toLowerCase()));
    }
}
