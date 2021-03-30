package io.arenadata.dtm.query.calcite.core.extension.ddl;

import io.arenadata.dtm.query.calcite.core.extension.parser.ParseException;
import io.arenadata.dtm.query.calcite.core.util.SqlNodeUtil;
import lombok.Getter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

@Getter
public class SqlAlterView extends SqlAlter {
    private final SqlIdentifier name;
    private final SqlNodeList columnList;
    private final SqlNode query;
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("VIEW", SqlKind.ALTER_VIEW);

    public SqlAlterView(SqlParserPos pos, SqlIdentifier name, SqlNodeList columnList, SqlNode query) throws ParseException {
        super(pos, OPERATOR.getName());
        this.name = Objects.requireNonNull(name);
        this.columnList = columnList;
        this.query = SqlNodeUtil.checkViewQueryAndGet(Objects.requireNonNull(query));
    }

    @Override
    protected void unparseAlterOperation(SqlWriter writer, int i, int i1) {
        this.name.unparse(writer, i, i1);
        writer.keyword("AS");
        writer.keyword(" ");
        this.query.unparse(writer, 0, 0);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(this.name, this.columnList, this.query);
    }
}
