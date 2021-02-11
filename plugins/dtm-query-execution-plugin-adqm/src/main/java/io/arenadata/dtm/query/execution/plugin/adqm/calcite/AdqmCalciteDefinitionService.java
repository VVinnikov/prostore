package io.arenadata.dtm.query.execution.plugin.adqm.calcite;

import io.arenadata.dtm.query.calcite.core.service.impl.CalciteDefinitionService;
import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adqmCalciteDefinitionService")
public class AdqmCalciteDefinitionService extends CalciteDefinitionService {

    @Autowired
    public AdqmCalciteDefinitionService(@Qualifier("adqmParserConfig") SqlParser.Config config) {
        super(config);
    }
}