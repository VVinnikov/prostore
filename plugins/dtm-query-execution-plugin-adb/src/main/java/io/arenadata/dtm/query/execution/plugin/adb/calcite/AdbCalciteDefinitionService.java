package io.arenadata.dtm.query.execution.plugin.adb.calcite;

import io.arenadata.dtm.query.calcite.core.service.impl.CalciteDefinitionService;
import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class AdbCalciteDefinitionService extends CalciteDefinitionService {

    @Autowired
    public AdbCalciteDefinitionService(@Qualifier("adbParserConfig") SqlParser.Config config) {
        super(config);
    }
}
