package io.arenadata.dtm.query.calcite.core.extension.dml;

import lombok.Getter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqlSelectExt extends SqlSelect {

    private SqlCharStringLiteral datasourceType;

    public SqlSelectExt(SqlParserPos pos,
                        SqlNodeList keywordList,
                        SqlNodeList selectList,
                        SqlNode from,
                        SqlNode where,
                        SqlNodeList groupBy,
                        SqlNode having,
                        SqlNodeList windowDecls,
                        SqlNodeList orderBy,
                        SqlNode offset,
                        SqlNode fetch,
                        SqlNodeList hints,
                        SqlNode datasourceType) {
        super(pos, keywordList, selectList, from, where, groupBy, having, windowDecls, orderBy, offset, fetch, hints);
        this.datasourceType = (SqlCharStringLiteral) datasourceType;
    }

}
