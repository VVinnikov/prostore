package io.arenadata.dtm.query.execution.plugin.adg.calcite.service;

import io.arenadata.dtm.query.calcite.core.factory.impl.CalciteSchemaFactory;
import io.arenadata.dtm.query.calcite.core.provider.CalciteContextProvider;
import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component("adgCalciteContextProvider")
public class AdgCalciteContextProvider extends CalciteContextProvider {
    public AdgCalciteContextProvider(@Qualifier("adgParserConfig") SqlParser.Config configParser,
                                     @Qualifier("adgCalciteSchemaFactory") CalciteSchemaFactory calciteSchemaFactory) {
        super(configParser, calciteSchemaFactory);
    }
}
