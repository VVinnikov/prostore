package io.arenadata.dtm.query.calcite.core.extension.snapshot;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;

import java.util.List;

public class SnapshotOperator extends SqlOperator {

    protected SnapshotOperator() {
        super("SNAPSHOT", SqlKind.SNAPSHOT, 2, true, (SqlReturnTypeInference) null, (SqlOperandTypeInference) null, (SqlOperandTypeChecker) null);
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.SPECIAL;
    }

    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode tableRef, SqlNode period,
                              SqlOperator started, SqlOperator finished, SqlNode num, SqlLiteral isLatestUncommitedDelta) {
        assert functionQualifier == null;
        return new SqlSnapshot(pos,
                tableRef, period, started, finished, num, isLatestUncommitedDelta);
    }

    public <R> void acceptCall(SqlVisitor<R> visitor, SqlCall call, boolean onlyExpressions, SqlBasicVisitor.ArgHandler<R> argHandler) {
        if (onlyExpressions) {
            List<SqlNode> operands = call.getOperandList();

            for (int i = 1; i < operands.size(); ++i) {
                argHandler.visitChild(visitor, call, i, (SqlNode) operands.get(i));
            }
        } else {
            super.acceptCall(visitor, call, false, argHandler);
        }
    }

    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlSnapshot snapshot =
                (SqlSnapshot) call;
        snapshot.getTableRef().unparse(writer, 0, 0);
        writer.keyword("FOR SYSTEM_TIME");
    }
}
