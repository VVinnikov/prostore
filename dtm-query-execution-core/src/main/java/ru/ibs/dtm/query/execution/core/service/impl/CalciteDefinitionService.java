package ru.ibs.dtm.query.execution.core.service.impl;

import lombok.SneakyThrows;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.core.service.DefinitionService;

@Service
public class CalciteDefinitionService implements DefinitionService<SqlNode> {

  private SqlParser.Config config;

  @Autowired
  public CalciteDefinitionService(@Qualifier("coreParserConfig") SqlParser.Config config) {
    this.config = config;
  }

  @SneakyThrows
  @Override
  public SqlNode processingQuery(String sql) {
    SqlParser parser = SqlParser.create(sql, config);
    return parser.parseQuery();
  }
}
