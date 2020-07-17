package ru.ibs.dtm.query.execution.plugin.adqm.calcite;

import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.calcite.core.factory.impl.CalciteSchemaFactory;
import ru.ibs.dtm.query.calcite.core.provider.CalciteContextProvider;

@Component("adqmCalciteContextProvider")
public class AdqmCalciteContextProvider extends CalciteContextProvider {
    public AdqmCalciteContextProvider(@Qualifier("adqmParserConfig") SqlParser.Config configParser,
                                      @Qualifier("adqmCalciteSchemaFactory") CalciteSchemaFactory calciteSchemaFactory) {
        super(configParser, calciteSchemaFactory);
    }
}
