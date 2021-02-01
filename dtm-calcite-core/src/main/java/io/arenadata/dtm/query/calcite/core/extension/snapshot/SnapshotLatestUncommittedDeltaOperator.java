package io.arenadata.dtm.query.calcite.core.extension.snapshot;

import lombok.Getter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

@Getter
public class SnapshotLatestUncommittedDeltaOperator extends SqlCall {

    private static final SqlOperator LATEST_UNCOMMITTED_DELTA_OPERATOR =
            new SqlSpecialOperator("LATEST_UNCOMMITTED_DELTA", SqlKind.OTHER_DDL);
    private final Boolean isLatestUncommittedDelta;
    private final SqlLiteral isLatestNode;

    public SnapshotLatestUncommittedDeltaOperator(SqlParserPos pos, SqlLiteral isLatest) {
        super(pos);
        isLatestNode = isLatest;
        this.isLatestUncommittedDelta = isLatest != null && isLatestNode.booleanValue();
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
