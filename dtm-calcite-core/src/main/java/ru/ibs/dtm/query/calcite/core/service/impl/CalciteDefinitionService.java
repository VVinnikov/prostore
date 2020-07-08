package ru.ibs.dtm.query.calcite.core.service.impl;

import lombok.SneakyThrows;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;

public class CalciteDefinitionService implements DefinitionService<SqlNode> {
    private SqlParser.Config config;

    public CalciteDefinitionService(SqlParser.Config config) {
        this.config = config;
    }

    @SneakyThrows
    public SqlNode processingQuery(String sql) {
        SqlParser parser = SqlParser.create(sql, config);
        return parser.parseQuery();
    }
}
