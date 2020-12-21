package io.arenadata.dtm.query.execution.core.calcite;

import io.arenadata.dtm.query.calcite.core.factory.impl.CalciteSchemaFactory;
import io.arenadata.dtm.query.calcite.core.provider.CalciteContextProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Slf4j
@Component("coreCalciteContextProvider")
public class CoreCalciteContextProvider extends CalciteContextProvider {

    @Autowired
    public CoreCalciteContextProvider(@Qualifier("coreParserConfig") SqlParser.Config configParser,
                                      @Qualifier("coreCalciteSchemaFactory") CalciteSchemaFactory calciteSchemaFactory) {
        super(configParser, calciteSchemaFactory);
    }
}
