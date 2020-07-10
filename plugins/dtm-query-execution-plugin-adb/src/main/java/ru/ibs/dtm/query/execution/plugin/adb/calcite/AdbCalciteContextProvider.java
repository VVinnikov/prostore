package ru.ibs.dtm.query.execution.plugin.adb.calcite;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.calcite.core.factory.impl.CalciteSchemaFactory;
import ru.ibs.dtm.query.calcite.core.provider.CalciteContextProvider;

@Slf4j
@Component("adbCalciteContextProvider")
public class AdbCalciteContextProvider extends CalciteContextProvider {
    public AdbCalciteContextProvider(@Qualifier("adbParserConfig") SqlParser.Config configParser,
                                     @Qualifier("adbCalciteSchemaFactory") CalciteSchemaFactory calciteSchemaFactory) {
        super(configParser, calciteSchemaFactory);
    }
}
