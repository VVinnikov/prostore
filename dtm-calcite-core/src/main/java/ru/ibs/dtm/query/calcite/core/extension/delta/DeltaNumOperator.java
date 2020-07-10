package ru.ibs.dtm.query.calcite.core.extension.delta;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Optional;

public class DeltaNumOperator extends SqlCall {

    private final Long num;

    private static final SqlOperator OPERATOR_DELTA =
            new SqlSpecialOperator("SET", SqlKind.OTHER_DDL);

    public DeltaNumOperator(SqlParserPos pos, SqlNumericLiteral num) {
        super(pos);
        this.num = Optional.ofNullable(num).map(c -> c.longValue(true)).orElse(null);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR_DELTA;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(this.getOperator().getName());
        writer.keyword(String.valueOf(this.num));
    }

    public Long getNum() {
        return num;
    }
}
