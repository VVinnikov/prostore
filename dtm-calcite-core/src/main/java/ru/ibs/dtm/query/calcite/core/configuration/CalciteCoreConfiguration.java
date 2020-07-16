package ru.ibs.dtm.query.calcite.core.configuration;

import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.util.SourceStringReader;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.query.calcite.core.extension.parser.SqlEddlParserImpl;

@Configuration
public class CalciteCoreConfiguration {

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
