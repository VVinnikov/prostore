package io.arenadata.dtm.query.calcite.core.extension.edml;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlRollbackCrashedWriteOps extends SqlCall {

    private static final SqlOperator ROLLBACK_CRASHED_WRITE_OPS_OPERATOR =
        new SqlSpecialOperator("ROLLBACK CRASHED_WRITE_OPERATIONS", SqlKind.ROLLBACK);

    public SqlRollbackCrashedWriteOps(SqlParserPos pos) {
        super(pos);
    }

    @Override
    public SqlOperator getOperator() {
        return ROLLBACK_CRASHED_WRITE_OPS_OPERATOR;
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
