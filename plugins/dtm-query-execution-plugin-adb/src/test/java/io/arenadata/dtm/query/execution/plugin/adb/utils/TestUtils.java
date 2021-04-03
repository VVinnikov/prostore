package io.arenadata.dtm.query.execution.plugin.adb.utils;

import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.service.AdbCalciteDefinitionService;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.configuration.CalciteConfiguration;
import org.apache.calcite.sql.SqlNode;

public class TestUtils {
    public static final CalciteConfiguration CALCITE_CONFIGURATION = new CalciteConfiguration();
    public static final DefinitionService<SqlNode> DEFINITION_SERVICE =
            new AdbCalciteDefinitionService(CALCITE_CONFIGURATION.configDdlParser(CALCITE_CONFIGURATION.ddlParserImplFactory()));

    private TestUtils() {
    }

}
