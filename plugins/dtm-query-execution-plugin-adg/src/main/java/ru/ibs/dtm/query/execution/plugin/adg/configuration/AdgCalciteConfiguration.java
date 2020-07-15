package ru.ibs.dtm.query.execution.plugin.adg.configuration;

import javax.annotation.PostConstruct;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.SourceStringReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AdgCalciteConfiguration {

    @PostConstruct
    public void init() {
        System.setProperty("calcite.default.charset", "UTF-8");
        System.setProperty("calcite.default.nationalcharset", "UTF-8");
        System.setProperty("calcite.default.collation.name", "UTF-8$ru_RU");
    }

    @Bean("adgParserConfig")
    public SqlParser.Config configDdlParser(@Qualifier("adgParser") SqlParserImplFactory factory) {
        return SqlParser.configBuilder()
                .setParserFactory(factory)
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setCaseSensitive(false)
                .setQuotedCasing(Casing.UNCHANGED)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
    }

    @Bean("adgParser")
    public SqlParserImplFactory ddlParserImplFactory() {
        return reader -> {
            final SqlDdlParserImpl parser = new SqlDdlParserImpl(reader);
            if (reader instanceof SourceStringReader) {
                final String sql = ((SourceStringReader) reader).getSourceString();
                parser.setOriginalSql(sql);
            }
            return parser;
        };
    }

    @Bean("adgSqlDialect")
    public SqlDialect adgSqlDialect() {
        SqlDialect.Context CONTEXT = SqlDialect.EMPTY_CONTEXT
                .withDatabaseProduct(SqlDialect.DatabaseProduct.UNKNOWN)
                .withIdentifierQuoteString("\"")
                .withUnquotedCasing(Casing.TO_LOWER)
                .withCaseSensitive(false)
                .withQuotedCasing(Casing.UNCHANGED);
        return new PostgresqlSqlDialect(CONTEXT);
    }
}
