package ru.ibs.dtm.query.execution.plugin.adb.configuration;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.SourceStringReader;
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
    return reader -> {
      final SqlDdlParserImpl parser = new SqlDdlParserImpl(reader);
      if (reader instanceof SourceStringReader) {
        final String sql = ((SourceStringReader) reader).getSourceString();
        parser.setOriginalSql(sql);
      }
      return parser;
    };
  }
}
