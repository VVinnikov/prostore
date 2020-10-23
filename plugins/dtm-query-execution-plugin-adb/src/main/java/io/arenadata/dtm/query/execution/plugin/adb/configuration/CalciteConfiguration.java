package io.arenadata.dtm.query.execution.plugin.adb.configuration;

import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.dialect.LimitSqlDialect;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
public class CalciteConfiguration {

    @PostConstruct
    public void init() {
        System.setProperty("calcite.default.charset", "UTF-8");
        System.setProperty("calcite.default.nationalcharset", "UTF-8");
        System.setProperty("calcite.default.collation.name", "UTF-8$ru_RU");
    }

    @Bean("adbParserConfig")
    public SqlParser.Config configDdlParser(@Qualifier("adbParser") SqlParserImplFactory factory) {
        return SqlParser.configBuilder()
            .setParserFactory(factory)
            .setConformance(SqlConformanceEnum.DEFAULT)
            .setCaseSensitive(false)
            .setQuotedCasing(Casing.UNCHANGED)
            .setUnquotedCasing(Casing.TO_LOWER)
            .setQuoting(Quoting.DOUBLE_QUOTE)
            .build();
    }

    @Bean("adbParser")
    public SqlParserImplFactory ddlParserImplFactory() {
        return new CalciteCoreConfiguration().eddlParserImplFactory();
    }

    @Bean("adbSqlDialect")
    public SqlDialect adbSqlDialect() {
        SqlDialect.Context CONTEXT = SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(SqlDialect.DatabaseProduct.POSTGRESQL)
            .withIdentifierQuoteString("")
            .withUnquotedCasing(Casing.TO_LOWER)
            .withCaseSensitive(false)
            .withQuotedCasing(Casing.UNCHANGED);
        return new LimitSqlDialect(CONTEXT);
    }
}
