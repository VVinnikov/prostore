package io.arenadata.dtm.query.calcite.core.extension.snapshot;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

public class SnapshotLatestUncommittedDeltaOperator extends SqlCall {

    private final Boolean isLatestUncommittedDelta;

    private static final SqlOperator LATEST_UNCOMMITTED_DELTA_OPERATOR =
            new SqlSpecialOperator("LATEST_UNCOMMITTED_DELTA", SqlKind.OTHER_DDL);

    public SnapshotLatestUncommittedDeltaOperator(SqlParserPos pos, SqlLiteral isLatest) {
        super(pos);
        this.isLatestUncommittedDelta = isLatest != null && isLatest.booleanValue();
    }

    @Override
    public SqlOperator getOperator() {
        return LATEST_UNCOMMITTED_DELTA_OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (isLatestUncommittedDelta) {
            writer.keyword(this.getOperator().getName());
        }
    }

    public Boolean getIsLatestUncommittedDelta() {
        return isLatestUncommittedDelta;
    }
}
