package io.arenadata.dtm.query.execution.plugin.adg.calcite.service;

import io.arenadata.dtm.query.calcite.core.service.impl.CalciteDefinitionService;
import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adgCalciteDefinitionService")
public class AdgCalciteDefinitionService extends CalciteDefinitionService {

    @Autowired
    public AdgCalciteDefinitionService(@Qualifier("adgParserConfig") SqlParser.Config config) {
        super(config);
    }
}
