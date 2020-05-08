package ru.ibs.dtm.query.execution.core.configuration.calcite;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.SourceStringReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.query.execution.core.calcite.eddl.parser.SqlEddlParserImpl;

import javax.annotation.PostConstruct;

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
