package io.arenadata.dtm.query.execution.plugin.adp.calcite.service;

import io.arenadata.dtm.query.calcite.core.service.impl.CalciteDefinitionService;
import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adpCalciteDefinitionService")
public class AdpCalciteDefinitionService extends CalciteDefinitionService {

    @Autowired
    public AdpCalciteDefinitionService(@Qualifier("adpParserConfig") SqlParser.Config config) {
        super(config);
    }
}
