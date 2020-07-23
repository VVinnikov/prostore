package ru.ibs.dtm.query.execution.core.calcite;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.calcite.core.factory.impl.CalciteSchemaFactory;
import ru.ibs.dtm.query.calcite.core.provider.CalciteContextProvider;

@Slf4j
@Component("coreCalciteContextProvider")
public class CoreCalciteContextProvider extends CalciteContextProvider {
    public CoreCalciteContextProvider(@Qualifier("coreParserConfig") SqlParser.Config configParser,
                                      @Qualifier("coreCalciteSchemaFactory") CalciteSchemaFactory calciteSchemaFactory) {
        super(configParser, calciteSchemaFactory);
    }
}
