package ru.ibs.dtm.query.execution.core.service.impl;

import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.calcite.core.service.impl.CalciteDefinitionService;

@Service("coreCalciteDefinitionService")
public class CoreCalciteDefinitionService extends CalciteDefinitionService {
    public CoreCalciteDefinitionService(@Qualifier("coreParserConfig") SqlParser.Config config) {
        super(config);
    }
}
