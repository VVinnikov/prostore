package ru.ibs.dtm.query.execution.core.configuration.calcite;

import javax.annotation.PostConstruct;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.SourceStringReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.common.service.DeltaService;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.calcite.core.service.DeltaQueryPreprocessor;
import ru.ibs.dtm.query.calcite.core.service.impl.DeltaQueryPreprocessorImpl;
import ru.ibs.dtm.query.execution.core.calcite.eddl.parser.SqlEddlParserImpl;

@Configuration
public class CalciteConfiguration {

    @PostConstruct
    public void init() {
        System.setProperty("calcite.default.charset", "UTF-8");
        System.setProperty("calcite.default.nationalcharset", "UTF-8");
        System.setProperty("calcite.default.collation.name", "UTF-8$ru_RU");
    }

    @Bean("coreParserConfig")
    public SqlParser.Config configEddlParser(@Qualifier("coreParser") SqlParserImplFactory factory) {
        return SqlParser.configBuilder()
                .setParserFactory(factory)
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setLex(Lex.MYSQL)
                .setCaseSensitive(false)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
    }

    @Bean
    public DeltaQueryPreprocessor deltaQueryPreprocessor(
            @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
            DeltaService deltaService
    ) {
        return new DeltaQueryPreprocessorImpl(definitionService, deltaService);
    }

    @Bean("coreParser")
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
