package io.arenadata.dtm.query.execution.plugin.adqm.configuration;

import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.dialect.LimitSqlDialect;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.ClickHouseSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.ConversionUtil;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.nio.charset.Charset;

@Configuration
public class CalciteConfiguration {
    private static final Charset DEFAULT_CHARSET = Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);

    @PostConstruct
    public void init() {
        System.setProperty("saffron.default.charset", DEFAULT_CHARSET.name());
        System.setProperty("saffron.default.nationalcharset", DEFAULT_CHARSET.name());
        System.setProperty("saffron.default.collation.name", String.format("%s$en_US", DEFAULT_CHARSET.name()));
    }

    @Bean("adqmParserConfig")
    public SqlParser.Config configDdlParser(@Qualifier("adqmParser") SqlParserImplFactory factory) {
        return SqlParser.configBuilder()
                .setParserFactory(factory)
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setCaseSensitive(false)
                .setQuotedCasing(Casing.UNCHANGED)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
    }

    @Bean("adqmParser")
    public SqlParserImplFactory ddlParserImplFactory() {
        return new CalciteCoreConfiguration().eddlParserImplFactory();
    }


    @Bean("adqmSqlDialect")
    public SqlDialect adgSqlDialect() {
        SqlDialect.Context CONTEXT = ClickHouseSqlDialect.DEFAULT_CONTEXT
                .withDatabaseProduct(SqlDialect.DatabaseProduct.CLICKHOUSE)
                .withIdentifierQuoteString("")
                .withUnquotedCasing(Casing.TO_LOWER)
                .withCaseSensitive(false)
                .withQuotedCasing(Casing.UNCHANGED);
        return new LimitSqlDialect(CONTEXT);
    }
}
