/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.dtm.query.execution.plugin.adqm.enrichment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adqm.base.factory.AdqmHelperTableNamesFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.factory.AdqmCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.factory.AdqmSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.service.AdqmCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.service.AdqmCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.plugin.adqm.enrichment.service.AdqmDmlQueryExtendService;
import io.arenadata.dtm.query.execution.plugin.adqm.enrichment.service.AdqmQueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adqm.enrichment.service.AdqmQueryGenerator;
import io.arenadata.dtm.query.execution.plugin.adqm.enrichment.service.AdqmSchemaExtender;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.AdqmQueryJoinConditionsCheckService;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.AdqmQueryJoinConditionsCheckServiceImpl;
import io.arenadata.dtm.query.execution.plugin.adqm.utils.DeltaTestUtils;
import io.arenadata.dtm.query.execution.plugin.adqm.utils.TestUtils;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import io.vertx.core.Vertx;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.arenadata.dtm.query.execution.plugin.adqm.utils.TestUtils.assertNormalizedEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
@ExtendWith(VertxExtension.class)
class AdqmQueryEnrichmentServiceImplTest {
    private static final String ENV_NAME = "local";
    private static Map<String, String> EXPECTED_SQLS;

    private QueryParserService queryParserService;
    private QueryEnrichmentService enrichService;
    private List<Datamart> datamarts;


    @BeforeAll
    static void loadFiles() throws JsonProcessingException {
        EXPECTED_SQLS = Collections.unmodifiableMap(DatabindCodec.mapper()
                .readValue(loadTextFromFile("sql/expectedDmlSqls.json"), new TypeReference<Map<String, String>>() {
                }));
    }

    @BeforeEach
    void setUp(Vertx vertx) throws JsonProcessingException {
        val parserConfig = TestUtils.CALCITE_CONFIGURATION.configDdlParser(
                TestUtils.CALCITE_CONFIGURATION.ddlParserImplFactory());
        val contextProvider = new AdqmCalciteContextProvider(
                parserConfig,
                new AdqmCalciteSchemaFactory(new AdqmSchemaFactory()));

        queryParserService = new AdqmCalciteDMLQueryParserService(contextProvider, vertx);
        val helperTableNamesFactory = new AdqmHelperTableNamesFactoryImpl();
        val queryExtendService = new AdqmDmlQueryExtendService(helperTableNamesFactory);
        val sqlDialect = TestUtils.CALCITE_CONFIGURATION.adqmSqlDialect();
        val relToSqlConverter = new DtmRelToSqlConverter(sqlDialect, false);

        AdqmQueryJoinConditionsCheckService conditionsCheckService = mock(AdqmQueryJoinConditionsCheckServiceImpl.class);
        when(conditionsCheckService.isJoinConditionsCorrect(any())).thenReturn(true);
        enrichService = new AdqmQueryEnrichmentService(
                queryParserService,
                contextProvider,
                new AdqmQueryGenerator(queryExtendService,
                        sqlDialect, relToSqlConverter),
                new AdqmSchemaExtender(helperTableNamesFactory));

        datamarts = DatabindCodec.mapper()
                .readValue(loadTextFromFile("schema/dml.json"), new TypeReference<List<Datamart>>() {
                });
    }

    @SneakyThrows
    private static String loadTextFromFile(String path) {
        try (InputStream inputStream = AdqmQueryEnrichmentService.class.getClassLoader().getResourceAsStream(path)) {
            assert inputStream != null;
            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
    }

    @Test
    void enrichWithDeltaNum(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithDeltaNum"));
    }

    @Test
    void shouldBeShardedWhenLocal(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));
        enrichRequest.setLocal(true);

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("shouldBeShardedWhenLocal"));
    }

    @Test
    void enrichWithDeltaFinishedIn(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaFinishedIn(1, 1),
                        DeltaTestUtils.deltaFinishedIn(2, 2)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithDeltaFinishedIn"));
    }

    @Test
    void enrichWithDeltaStartedIn(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaStartedIn(1, 1),
                        DeltaTestUtils.deltaStartedIn(2, 2)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithDeltaStartedIn"));
    }

    @Test
    void enrichWithDeltaOnDate(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaOnDate(1),
                        DeltaTestUtils.deltaOnDate(2)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithDeltaOnDate"));
    }

    @Test
    void enrichWithWithoutDelta(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaWithout(1),
                        DeltaTestUtils.deltaWithout(2)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithWithoutDelta"));
    }

    @Test
    void enrichWithDeltaNum2(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithDeltaNum2"));
    }

    @Test
    void enrichWithDeltaNum3(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "select *, CASE WHEN (account_type = 'D' AND  amount >= 0) " +
                        "OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END\n" +
                        "  from (\n" +
                        "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                        "    from shares.accounts a\n" +
                        "    left join shares.transactions t using(account_id)\n" +
                        "   group by a.account_id, account_type\n" +
                        ")x",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithDeltaNum3"));
    }

    @Test
    void enrichWithDeltaNum4(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas("SELECT * FROM shares.transactions as tran",
                Arrays.asList(DeltaTestUtils.deltaNum(1)));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithDeltaNum4"));
    }

    @Test
    void enrichWithDeltaNum5(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT a1.account_id\n" +
                        "FROM (SELECT a2.account_id FROM shares.accounts a2 where a2.account_id = 12) a1\n" +
                        "    INNER JOIN shares.transactions t1 ON a1.account_id = t1.account_id",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithDeltaNum5"));
    }

    @Test
    void enrichWithDeltaNum6(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id " +
                        "LIMIT 10",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithDeltaNum6"));
    }

    @Test
    void enrichCount(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas("SELECT count(*) FROM shares.accounts",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichCount"));
    }

    @Test
    void enrichWithDeltaNum9(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas("SELECT * FROM shares.transactions where account_id = 1",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithDeltaNum9"));
    }

    @Test
    void enrichWithAggregate(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas("SELECT min(int_col) as min_col, min(double_col) as max_col, varchar_col\n" +
                        "FROM dml.AGGREGATION_TABLE\n" +
                        "group by varchar_col\n" +
                        "order by varchar_col\n" +
                        "limit 2",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithAggregate"));
    }

    @Test
    void enrichWithAggregate2(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas("SELECT min(int_col) as min_col, min(double_col) as max_col, varchar_col, NULL as t1\n" +
                        "FROM dml.AGGREGATION_TABLE\n" +
                        "where varchar_col = 'ф'\n" +
                        "group by varchar_col\n" +
                        "limit 2",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithAggregate2"));
    }

    @Test
    void enrichWithSort(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT COUNT(c.category_name),\n" +
                        "       c.category_name,\n" +
                        "       sum(p.units_in_stock),\n" +
                        "       c.id\n" +
                        "FROM dml.products p\n" +
                        "         JOIN dml.categories c on p.category_id = c.id\n" +
                        "GROUP BY c.category_name, c.id\n" +
                        "ORDER BY c.id\n" +
                        "limit 5",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithSort"));
    }

    @Test
    void enrichWithSort3(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT COUNT(dml.categories.category_name),\n" +
                        "       dml.categories.category_name,\n" +
                        "       dml.categories.id,\n" +
                        "       sum(dml.products.units_in_stock)\n" +
                        "FROM dml.products\n" +
                        "         INNER JOIN dml.categories on dml.products.category_id = dml.categories.id\n" +
                        "GROUP BY dml.categories.category_name, dml.categories.id\n" +
                        "ORDER BY dml.categories.id limit 5",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithSort3"));
    }

    @Test
    void enrichWithSort4(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT COUNT(dml.categories.category_name),\n" +
                        "       sum(dml.products.units_in_stock)\n" +
                        "FROM dml.products\n" +
                        "         INNER JOIN dml.categories on dml.products.category_id = dml.categories.id\n" +
                        "GROUP BY dml.categories.category_name, dml.categories.id\n" +
                        "ORDER BY dml.categories.id limit 5",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithSort4"));
    }

    @Test
    void enrichWithSort5(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT c.id \n" +
                        "FROM dml.products p\n" +
                        "         JOIN dml.categories c on p.category_id = c.id\n" +
                        "ORDER BY c.id",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithSort5"));
    }

    @Test
    void enrichWithSort6(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT *\n" +
                        "from dml.categories c\n" +
                        "         JOIN (select * from  dml.products) p on c.id = p.category_id\n" +
                        "ORDER by c.id, p.product_name desc",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                )
        );

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithSort6"));
    }

    @Test
    void enrichWithManyInKeyword(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestWithDeltas("SELECT account_id FROM shares.accounts WHERE account_id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)",
                Arrays.asList(DeltaTestUtils.deltaNum(1)));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, EXPECTED_SQLS.get("enrichWithManyInKeyword"));
    }

    @Test
    void enrichWithCustomSelect(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestWithDeltas("SELECT *, 0 FROM shares.accounts ORDER BY account_id",
                Arrays.asList(DeltaTestUtils.deltaNum(1)));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, EXPECTED_SQLS.get("enrichWithCustomSelect"));
    }

    @Test
    void enrichWithSubquery(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestWithDeltas("SELECT * FROM shares.accounts as b where b.account_id IN (select c.account_id from shares.transactions as c limit 1)",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(2),
                        DeltaTestUtils.deltaNum(1)
                )
        );

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, EXPECTED_SQLS.get("enrichWithSubquery"));
    }

    @Test
    void enrichWithSubqueryInJoin(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestWithDeltas("SELECT * FROM shares.accounts as b JOIN (select c.account_id from shares.transactions as c) t ON b.account_id=t.account_id WHERE b.account_id > 0 LIMIT 1",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(2)
                )
        );

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, EXPECTED_SQLS.get("enrichWithSubqueryInJoin"));
    }

    @Test
    void enrichWithAliasesAndFunctions(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestWithDeltas("SELECT *\n" +
                        "FROM (SELECT\n" +
                        "          account_id      as bc1,\n" +
                        "          true            as bc2,\n" +
                        "          abs(account_id) as bc3,\n" +
                        "          'some$' as bc4,\n" +
                        "          '$some'\n" +
                        "      FROM shares.accounts) t1\n" +
                        "               JOIN shares.transactions as t2\n" +
                        "                    ON t1.bc1 = t2.account_id AND abs(t2.account_id) = t1.bc3\n" +
                        "WHERE t1.bc2 = true AND t1.bc3 = 0",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(2),
                        DeltaTestUtils.deltaNum(1)
                )
        );

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, EXPECTED_SQLS.get("enrichWithAliasesAndFunctions"));
    }

    @Test
    void shouldFailWhenNotEnoughDeltas(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        queryParserService.parse(new QueryParserRequest(enrichRequest.getQuery(), enrichRequest.getSchema()))
                .compose(parserResponse -> enrichService.enrich(enrichRequest, parserResponse))
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        Assertions.fail("Unexpected success");
                    }

                    Assertions.assertSame(DataSourceException.class, ar.cause().getClass());
                }).completeNow());
    }

    private void enrichAndAssert(VertxTestContext testContext, EnrichQueryRequest enrichRequest,
                                 String expectedSql) {
        queryParserService.parse(new QueryParserRequest(enrichRequest.getQuery(), enrichRequest.getSchema()))
                .compose(parserResponse -> enrichService.enrich(enrichRequest, parserResponse))
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        Assertions.fail(ar.cause());
                    }

                    assertNormalizedEquals(ar.result(), expectedSql);
                }).completeNow());
    }

    private EnrichQueryRequest prepareRequestWithDeltas(String sql, List<DeltaInformation> deltaInformations) {
        return EnrichQueryRequest.builder()
                .query(TestUtils.DEFINITION_SERVICE.processingQuery(sql))
                .deltaInformations(deltaInformations)
                .envName(ENV_NAME)
                .schema(datamarts)
                .build();
    }
}
