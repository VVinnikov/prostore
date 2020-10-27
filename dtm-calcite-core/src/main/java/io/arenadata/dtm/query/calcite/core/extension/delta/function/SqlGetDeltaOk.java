package io.arenadata.dtm.query.calcite.core.extension.delta.function;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlGetDeltaOk extends SqlCall {

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("GET_DELTA_OK", SqlKind.OTHER_DDL);

    public SqlGetDeltaOk(SqlParserPos pos) {
        super(pos);
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
        writer.literal(OPERATOR + "()");
    }
}
