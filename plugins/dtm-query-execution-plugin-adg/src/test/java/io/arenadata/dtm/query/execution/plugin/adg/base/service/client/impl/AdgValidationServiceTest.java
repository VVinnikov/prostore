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
package io.arenadata.dtm.query.execution.plugin.adg.base.service.client.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.configuration.AdgCalciteConfiguration;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.factory.AdgCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.factory.AdgSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.service.AdgCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.service.AdgCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.plugin.adg.dml.service.AdgValidationService;
import io.arenadata.dtm.query.execution.plugin.adg.utils.TestUtils;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrValidationService;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AdgValidationServiceTest {

    private final LlrValidationService validationService = new AdgValidationService();
    private QueryParserService queryParserService;

    @BeforeEach
    void setUp() {
        val calciteConfiguration = new AdgCalciteConfiguration();
        calciteConfiguration.init();
        val parserConfig = calciteConfiguration.configDdlParser(
                calciteConfiguration.ddlParserImplFactory());
        val contextProvider = new AdgCalciteContextProvider(
                parserConfig,
                new AdgCalciteSchemaFactory(new AdgSchemaFactory()));
        queryParserService = new AdgCalciteDMLQueryParserService(contextProvider, Vertx.vertx());
    }

    @Test
    void validateInnerJoinSuccess() throws InterruptedException {
        List<Datamart> datamarts = Collections.singletonList(getSchema("shares", true));
        String sql = "SELECT * FROM shares.accounts a INNER JOIN shares.transactions t ON a.account_id = t.account_id";
        val testContext = new VertxTestContext();
        queryParserService.parse(new QueryParserRequest(TestUtils.DEFINITION_SERVICE.processingQuery(sql), datamarts))
                .map(parserResponse -> {
                    validationService.validate(parserResponse);
                    return parserResponse;
                })
                .onSuccess(result -> testContext.completeNow());
        assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
        assertFalse(testContext.failed());
    }

    @Test
    void validateFullJoinFail() throws InterruptedException {
        List<Datamart> datamarts = Collections.singletonList(getSchema("shares", true));
        String sql = "SELECT * FROM shares.accounts a FULL JOIN shares.transactions t ON a.account_id = t.account_id";
        val testContext = new VertxTestContext();
        queryParserService.parse(new QueryParserRequest(TestUtils.DEFINITION_SERVICE.processingQuery(sql), datamarts))
                .map(parserResponse -> {
                    validationService.validate(parserResponse);
                    return parserResponse;
                })
                .onFailure(testContext::failNow);
        assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
        assertTrue(testContext.causeOfFailure().getMessage().contains("FULL"));
    }

    private Datamart getSchema(String schemaName, boolean isDefault) {
        Entity accounts = Entity.builder()
                .schema(schemaName)
                .name("accounts")
                .build();
        List<EntityField> accAttrs = Arrays.asList(
                EntityField.builder()
                        .type(ColumnType.BIGINT)
                        .name("account_id")
                        .ordinalPosition(1)
                        .shardingOrder(1)
                        .primaryOrder(1)
                        .nullable(false)
                        .accuracy(null)
                        .size(null)
                        .build(),
                EntityField.builder()
                        .type(ColumnType.VARCHAR)
                        .name("account_type")
                        .ordinalPosition(2)
                        .shardingOrder(null)
                        .primaryOrder(null)
                        .nullable(false)
                        .accuracy(null)
                        .size(1)
                        .build()
        );
        accounts.setFields(accAttrs);

        Entity transactions = Entity.builder()
                .schema(schemaName)
                .name("transactions")
                .build();

        List<EntityField> trAttr = Arrays.asList(
                EntityField.builder()
                        .type(ColumnType.BIGINT)
                        .name("transaction_id")
                        .ordinalPosition(1)
                        .shardingOrder(1)
                        .primaryOrder(1)
                        .nullable(false)
                        .accuracy(null)
                        .size(null)
                        .build(),
                EntityField.builder()
                        .type(ColumnType.DATE)
                        .name("transaction_date")
                        .ordinalPosition(2)
                        .shardingOrder(null)
                        .primaryOrder(null)
                        .nullable(true)
                        .accuracy(null)
                        .size(null)
                        .build(),
                EntityField.builder()
                        .type(ColumnType.BIGINT)
                        .name("account_id")
                        .ordinalPosition(3)
                        .shardingOrder(1)
                        .primaryOrder(2)
                        .nullable(false)
                        .accuracy(null)
                        .size(null)
                        .build(),
                EntityField.builder()
                        .type(ColumnType.BIGINT)
                        .name("amount")
                        .ordinalPosition(4)
                        .shardingOrder(null)
                        .primaryOrder(null)
                        .nullable(false)
                        .accuracy(null)
                        .size(null)
                        .build()
        );

        transactions.setFields(trAttr);

        return new Datamart(schemaName, isDefault, Arrays.asList(transactions, accounts));
    }
}
