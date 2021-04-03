package io.arenadata.dtm.query.execution.plugin.adqm.calcite.service;

import io.arenadata.dtm.query.calcite.core.factory.impl.CalciteSchemaFactory;
import io.arenadata.dtm.query.calcite.core.provider.CalciteContextProvider;
import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component("adqmCalciteContextProvider")
public class AdqmCalciteContextProvider extends CalciteContextProvider {
    public AdqmCalciteContextProvider(@Qualifier("adqmParserConfig") SqlParser.Config configParser,
                                      @Qualifier("adqmCalciteSchemaFactory") CalciteSchemaFactory calciteSchemaFactory) {
        super(configParser, calciteSchemaFactory);
    }
}
