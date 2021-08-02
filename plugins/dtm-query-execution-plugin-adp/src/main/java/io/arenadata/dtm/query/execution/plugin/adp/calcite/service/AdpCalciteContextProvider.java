package io.arenadata.dtm.query.execution.plugin.adp.calcite.service;

import io.arenadata.dtm.query.calcite.core.factory.impl.CalciteSchemaFactory;
import io.arenadata.dtm.query.calcite.core.provider.CalciteContextProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Slf4j
@Component("adpCalciteContextProvider")
public class AdpCalciteContextProvider extends CalciteContextProvider {

    @Autowired
    public AdpCalciteContextProvider(@Qualifier("adpParserConfig") SqlParser.Config configParser,
                                     @Qualifier("adpCalciteSchemaFactory") CalciteSchemaFactory calciteSchemaFactory) {
        super(configParser, calciteSchemaFactory);
    }
}
