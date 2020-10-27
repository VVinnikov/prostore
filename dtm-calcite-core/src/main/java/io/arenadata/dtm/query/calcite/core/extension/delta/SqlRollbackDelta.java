package io.arenadata.dtm.query.calcite.core.extension.delta;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlRollbackDelta extends SqlCall {

    private static final SqlOperator ROLLBACK_DELTA_OPERATOR =
        new SqlSpecialOperator("ROLLBACK DELTA", SqlKind.OTHER_DDL);


    public SqlRollbackDelta(SqlParserPos pos) {
        super(pos);
    }

    @Override
    public SqlOperator getOperator() {
        return ROLLBACK_DELTA_OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(this.getOperator().getName());
    }
}
