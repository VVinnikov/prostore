package ru.ibs.dtm.query.execution.core.calcite.delta;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlCommitDelta extends SqlCall {

    private DeltaDateTimeOperator deltaDateTimeOperator;
    private static final SqlOperator COMMIT_DELTA_OPERATOR =
            new SqlSpecialOperator("COMMIT DELTA", SqlKind.OTHER_DDL);

    public SqlCommitDelta(SqlParserPos pos, SqlNode dateTime) {
        super(pos);
        this.deltaDateTimeOperator = new DeltaDateTimeOperator(pos, (SqlCharStringLiteral) dateTime);
    }

    @Override
    public SqlOperator getOperator() {
        return COMMIT_DELTA_OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(deltaDateTimeOperator);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(this.getOperator().getName());
        deltaDateTimeOperator.unparse(writer, leftPrec, rightPrec);
    }

    public DeltaDateTimeOperator getDeltaDateTimeOperator() {
        return deltaDateTimeOperator;
    }
}
