package ru.ibs.dtm.query.execution.plugin.adqm.calcite;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DefinitionService;

@Service
public class CalciteDefinitionService implements DefinitionService<SqlNode> {

    private SqlParser.Config config;

    @Autowired
    public CalciteDefinitionService(@Qualifier("adqmParserConfig") SqlParser.Config config) {
        this.config = config;
    }

    public SqlNode processingQuery(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql, config);
        return parser.parseQuery();
    }
}
