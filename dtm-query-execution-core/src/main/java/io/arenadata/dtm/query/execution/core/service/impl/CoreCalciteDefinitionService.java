package io.arenadata.dtm.query.execution.core.service.impl;

import io.arenadata.dtm.query.calcite.core.service.impl.CalciteDefinitionService;
import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("coreCalciteDefinitionService")
public class CoreCalciteDefinitionService extends CalciteDefinitionService {
    public CoreCalciteDefinitionService(@Qualifier("coreParserConfig") SqlParser.Config config) {
        super(config);
    }
}
