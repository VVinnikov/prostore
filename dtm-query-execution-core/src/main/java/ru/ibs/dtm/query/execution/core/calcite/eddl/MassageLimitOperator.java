package ru.ibs.dtm.query.execution.core.calcite.eddl;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Optional;

public class MassageLimitOperator extends SqlCall {

    private final Integer messageLimit;

    private static final SqlOperator OPERATOR_MESSAGE_LIMIT =
            new SqlSpecialOperator("MESSAGE_LIMIT", SqlKind.OTHER_DDL);

    public MassageLimitOperator(SqlParserPos pos, SqlNumericLiteral messageLimit) {
        super(pos);
        this.messageLimit = Optional.ofNullable(messageLimit).map(c -> c.intValue(true)).orElse(null);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR_MESSAGE_LIMIT;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

    public Integer getMessageLimit() {
        return messageLimit;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(this.getOperator().getName());
        writer.keyword(String.valueOf(this.messageLimit));
    }
}