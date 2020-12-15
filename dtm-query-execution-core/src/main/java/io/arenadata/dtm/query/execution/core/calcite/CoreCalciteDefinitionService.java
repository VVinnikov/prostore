package io.arenadata.dtm.query.execution.core.calcite;

import io.arenadata.dtm.query.calcite.core.service.impl.CalciteDefinitionService;
import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class CoreCalciteDefinitionService extends CalciteDefinitionService {

    @Autowired
    public CoreCalciteDefinitionService(@Qualifier("coreParserConfig") SqlParser.Config config) {
        super(config);
    }
}
