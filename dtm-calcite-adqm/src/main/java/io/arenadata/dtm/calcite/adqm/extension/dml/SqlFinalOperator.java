package io.arenadata.dtm.calcite.adqm.extension.dml;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;

import java.util.List;

public class SqlFinalOperator extends SqlOperator {

    public static final String NAME = "FINAL";

    protected SqlFinalOperator() {
        super(NAME,
                SqlKind.FINAL,
                2,
                true,
                (SqlReturnTypeInference) null,
                (SqlOperandTypeInference) null,
                (SqlOperandTypeChecker) null);
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.SPECIAL;
    }

    public SqlFinalTable createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode tableRef) {
        assert functionQualifier == null;
        return new SqlFinalTable(pos, tableRef);
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
        SqlFinalTable finalTable = (SqlFinalTable) call;
        finalTable.getTableRef().unparse(writer, 0, 0);
        writer.keyword(NAME);
    }
}
