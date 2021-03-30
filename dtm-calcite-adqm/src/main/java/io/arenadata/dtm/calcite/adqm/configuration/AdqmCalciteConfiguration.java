package io.arenadata.dtm.calcite.adqm.configuration;

import io.arenadata.dtm.calcite.adqm.extension.parser.SqlEddlParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.util.SourceStringReader;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AdqmCalciteConfiguration {

    public SqlParserImplFactory eddlParserImplFactory() {
        return reader -> {
            final SqlEddlParserImpl parser = new SqlEddlParserImpl(reader);
            if (reader instanceof SourceStringReader) {
                final String sql = ((SourceStringReader) reader).getSourceString();
                parser.setOriginalSql(sql);
            }
            return parser;
        };
    }
}
