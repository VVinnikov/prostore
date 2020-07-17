package ru.ibs.dtm.query.execution.plugin.adg.calcite;

import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.calcite.core.factory.impl.CalciteSchemaFactory;
import ru.ibs.dtm.query.calcite.core.provider.CalciteContextProvider;

@Component("adgCalciteContextProvider")
public class AdgCalciteContextProvider extends CalciteContextProvider {
    public AdgCalciteContextProvider(@Qualifier("adgParserConfig") SqlParser.Config configParser,
                                     @Qualifier("adgCalciteSchemaFactory") CalciteSchemaFactory calciteSchemaFactory) {
        super(configParser, calciteSchemaFactory);
    }
}
