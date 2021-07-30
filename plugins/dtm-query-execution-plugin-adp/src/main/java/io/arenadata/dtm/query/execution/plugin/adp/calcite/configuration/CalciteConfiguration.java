package io.arenadata.dtm.query.execution.plugin.adp.calcite.configuration;

import io.arenadata.dtm.query.calcite.core.dialect.LimitSqlDialect;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.util.ConversionUtil;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.charset.Charset;

@Configuration
public class CalciteConfiguration {

    private static final Charset DEFAULT_CHARSET = Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);

    static  {
        System.setProperty("saffron.default.charset", DEFAULT_CHARSET.name());
        System.setProperty("saffron.default.nationalcharset", DEFAULT_CHARSET.name());
        System.setProperty("saffron.default.collation.name", String.format("%s$en_US", DEFAULT_CHARSET.name()));
    }

    @Bean("adpSqlDialect")
    public SqlDialect adpSqlDialect() {
        SqlDialect.Context CONTEXT = SqlDialect.EMPTY_CONTEXT
                .withDatabaseProduct(SqlDialect.DatabaseProduct.POSTGRESQL)
                .withIdentifierQuoteString("")
                .withUnquotedCasing(Casing.TO_LOWER)
                .withCaseSensitive(false)
                .withQuotedCasing(Casing.UNCHANGED);
        return new LimitSqlDialect(CONTEXT);
    }

}
