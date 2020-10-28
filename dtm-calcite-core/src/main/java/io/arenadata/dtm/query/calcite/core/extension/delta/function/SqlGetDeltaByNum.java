package io.arenadata.dtm.query.calcite.core.extension.delta.function;

import io.arenadata.dtm.query.calcite.core.extension.delta.SqlDeltaCall;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class SqlGetDeltaByNum extends SqlDeltaCall {

    private final Long deltaNum;
    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("GET_DELTA_BY_NUM", SqlKind.OTHER_DDL);

    public SqlGetDeltaByNum(SqlParserPos pos, SqlNode deltaNum) {
        super(pos);
        this.deltaNum = Optional.ofNullable((SqlNumericLiteral) deltaNum)
                .map(c -> c.longValue(true)).orElse(null);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.literal(
                OPERATOR + "(" + this.deltaNum + ")");
    }

    public Long getDeltaNum() {
        return deltaNum;
    }
}
