package ru.ibs.dtm.query.calcite.core.extension.snapshot;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

public class SnapshotLatestUncommitedDeltaOperator  extends SqlCall {

    private final Boolean isLatestUncommitedDelta;

    private static final SqlOperator LATEST_UNCOMMITED_DELTA_OPERATOR =
            new SqlSpecialOperator("LATEST_UNCOMMITED_DELTA", SqlKind.OTHER_DDL);

    public SnapshotLatestUncommitedDeltaOperator(SqlParserPos pos, SqlLiteral isLatest) {
        super(pos);
        this.isLatestUncommitedDelta = isLatest != null && isLatest.booleanValue();
    }

    @Override
    public SqlOperator getOperator() {
        return LATEST_UNCOMMITED_DELTA_OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (isLatestUncommitedDelta) {
            writer.keyword(this.getOperator().getName());
        }
    }

    public Boolean getIsLatestUncommitedDelta() {
        return isLatestUncommitedDelta;
    }
}
