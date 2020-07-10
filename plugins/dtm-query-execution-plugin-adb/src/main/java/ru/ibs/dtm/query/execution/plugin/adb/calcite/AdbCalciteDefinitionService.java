package ru.ibs.dtm.query.execution.plugin.adb.calcite;

import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.calcite.core.service.impl.CalciteDefinitionService;

@Service
public class AdbCalciteDefinitionService extends CalciteDefinitionService {
    public AdbCalciteDefinitionService(@Qualifier("adbParserConfig") SqlParser.Config config) {
        super(config);
    }
}
