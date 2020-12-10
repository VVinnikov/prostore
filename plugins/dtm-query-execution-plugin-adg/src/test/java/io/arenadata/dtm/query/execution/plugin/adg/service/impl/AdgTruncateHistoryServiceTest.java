package io.arenadata.dtm.query.execution.plugin.adg.service.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.AdgCalciteConfiguration;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.service.impl.ddl.AdgTruncateHistoryService;
import io.arenadata.dtm.query.execution.plugin.adg.utils.AdgUtils;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryParams;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields.ACTUAL_POSTFIX;
import static io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields.HISTORY_POSTFIX;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class AdgTruncateHistoryServiceTest {
    private static final String ENV = "env";
    private static final String SCHEMA = "schema";
    private static final String TABLE = "table";
    private final AdgCalciteConfiguration calciteConfiguration = new AdgCalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration
            .configDdlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
    private final FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
    private final Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
    private final AdgCartridgeClient adgCartridgeClient = mock(AdgCartridgeClientImpl.class);
    private final TruncateHistoryService adgTruncateHistoryService = new AdgTruncateHistoryService(adgCartridgeClient,
            calciteConfiguration.adgSqlDialect());

    @BeforeEach
    void setUp() {
        when(adgCartridgeClient.deleteSpaceTuples(anyString(), anyString())).thenReturn(Future.succeededFuture());
    }

    @Test
    void test() {
        List<Pair<String, String>> expectedList = Arrays.asList(
                new Pair<>(AdgUtils.getSpaceName(ENV, SCHEMA, TABLE, ACTUAL_POSTFIX), null),
                new Pair<>(AdgUtils.getSpaceName(ENV, SCHEMA, TABLE, HISTORY_POSTFIX), null)
        );
        test(null, null, expectedList);
    }

    @Test
    void testWithConditions() {
        String conditions = "id > 2";
        String expectedCondition = "(\"id\" > 2)";
        List<Pair<String, String>> expectedList = Arrays.asList(
                new Pair<>(AdgUtils.getSpaceName(ENV, SCHEMA, TABLE, ACTUAL_POSTFIX), expectedCondition),
                new Pair<>(AdgUtils.getSpaceName(ENV, SCHEMA, TABLE, HISTORY_POSTFIX), expectedCondition)
        );
        test(null, conditions, expectedList);
    }

    @Test
    void testWithSysCn() {
        Long sysCn = 1L;
        test(sysCn, null, Collections.singletonList(
                new Pair<>(AdgUtils.getSpaceName(ENV, SCHEMA, TABLE, HISTORY_POSTFIX),
                        String.format("\"sys_to\" < %s", sysCn)
                )));
    }


    @Test
    void testWithConditionsAndSysCn() {
        String conditions = "id > 2";
        Long sysCn = 1L;
        String expected = String.format("(%s) AND \"sys_to\" < %s", "\"id\" > 2", sysCn);
        test(sysCn, conditions, Collections.singletonList(
                new Pair<>(AdgUtils.getSpaceName(ENV, SCHEMA, TABLE, HISTORY_POSTFIX), expected)));
    }

    private void test(Long sysCn, String conditions, List<Pair<String, String>> expectedList) {
        adgTruncateHistoryService.truncateHistory(getParams(sysCn, conditions));
        expectedList.forEach(pair -> verify(adgCartridgeClient, times(1))
                .deleteSpaceTuples(eq(pair.getKey()), eq(pair.getValue())));
        verify(adgCartridgeClient, times(expectedList.size()))
                .deleteSpaceTuples(anyString(), any());
    }

    private TruncateHistoryParams getParams(Long sysCn, String conditions) {
        Entity entity = new Entity();
        entity.setSchema(SCHEMA);
        entity.setName(TABLE);
        SqlNode sqlNode = Optional.ofNullable(conditions)
                .map(val -> {
                    try {
                        return ((SqlSelect) planner.parse(String.format("SELECT * from t WHERE %s", conditions)))
                                .getWhere();
                    } catch (SqlParseException e) {
                        throw new RuntimeException(e);
                    }
                })
                .orElse(null);
        return new TruncateHistoryParams(null, null, sysCn, entity, ENV, sqlNode);
    }
}