package io.arenadata.dtm.query.execution.core.utils;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.dialect.LimitSqlDialect;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.base.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.core.base.configuration.properties.CoreDtmSettings;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteDefinitionService;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.*;

public class TestUtils {
    public static final CalciteConfiguration CALCITE_CONFIGURATION = new CalciteConfiguration();
    public static final CalciteCoreConfiguration CALCITE_CORE_CONFIGURATION = new CalciteCoreConfiguration();
    public static final DefinitionService<SqlNode> DEFINITION_SERVICE =
            new CoreCalciteDefinitionService(CALCITE_CONFIGURATION.configEddlParser(CALCITE_CORE_CONFIGURATION.eddlParserImplFactory()));
    public static final SqlDialect SQL_DIALECT = new LimitSqlDialect(SqlDialect.EMPTY_CONTEXT);
    public static final CoreDtmSettings CORE_DTM_SETTINGS = new CoreDtmSettings(ZoneId.of("UTC"));


    private TestUtils() {
    }

    public static AppConfiguration getCoreConfiguration(String envName) {
        return getCoreAppConfiguration(CORE_DTM_SETTINGS, envName);
    }

    public static AppConfiguration getCoreAppConfiguration(DtmConfig dtmSettings, String envName) {
        return new AppConfiguration(null) {
            @Override
            public String getEnvName() {
                return envName;
            }

            @Override
            public DtmConfig dtmSettings() {
                return dtmSettings;
            }
        };
    }

    public static void assertException(Class<? extends Throwable> expected, String partOfMessage, Throwable actual) {
        assertNotNull(actual);
        assertSame(expected, actual.getClass());
        assertTrue(actual.getMessage().contains(partOfMessage), String.format("Message: %s\nNot contains expected part of message: %s", actual.getMessage(), partOfMessage));
    }
}
