package io.arenadata.dtm.query.calcite.core.service.impl;

import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import lombok.SneakyThrows;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;

public abstract class CalciteDefinitionService implements DefinitionService<SqlNode> {
    private final SqlParser.Config config;

    public CalciteDefinitionService(SqlParser.Config config) {
        this.config = config;
    }

    @SneakyThrows
    public SqlNode processingQuery(String sql) {
        SqlParser parser = SqlParser.create(sql, config);
        return parser.parseQuery();
    }
}
