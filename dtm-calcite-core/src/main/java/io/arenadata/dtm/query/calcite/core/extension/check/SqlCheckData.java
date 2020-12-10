package io.arenadata.dtm.query.calcite.core.extension.check;

import io.arenadata.dtm.query.calcite.core.util.CalciteUtil;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SqlCheckData extends SqlCheckCall {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CHECK_DATA", SqlKind.CHECK);
    private final String table;
    private final String schema;
    private final Long deltaNum;
    private final Set<String> columns;

    public SqlCheckData(SqlParserPos pos, SqlIdentifier name, SqlLiteral deltaNum, List<SqlNode> columns) {
        super(pos, name);
        this.schema = CalciteUtil.parseSchemaName(name.toString());
        this.table = CalciteUtil.parseTableName(name.toString());
        this.deltaNum = deltaNum.longValue(true);
        this.columns = Optional.ofNullable(columns)
                .map(val -> (columns.stream().map(c -> ((SqlIdentifier) c).toString())
                        .collect(Collectors.toSet())))
                .orElse(null);
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public CheckType getType() {
        return CheckType.DATA;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public Long getDeltaNum() {
        return deltaNum;
    }

    public Set<String> getColumns() {
        return columns;
    }
}
