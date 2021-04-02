package io.arenadata.dtm.query.execution.plugin.adqm.utils;

import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.service.impl.CalciteDefinitionService;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.configuration.CalciteConfiguration;
import org.apache.calcite.sql.SqlNode;

public class TestUtils {
    public static final CalciteConfiguration CALCITE_CONFIGURATION = new CalciteConfiguration();
    public static final DefinitionService<SqlNode> DEFINITION_SERVICE =
            new CalciteDefinitionService(CALCITE_CONFIGURATION.configDdlParser(CALCITE_CONFIGURATION.ddlParserImplFactory())) {
            };
}
