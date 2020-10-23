package io.arenadata.dtm.query.calcite.core.extension.snapshot;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

public class SnapshotStartedOperator extends SnapshotDeltaIntervalOperator {

    private static final SqlOperator STARTED_IN_OPERATOR =
            new SqlSpecialOperator("STARTED IN", SqlKind.OTHER_DDL);

    public SnapshotStartedOperator(SqlParserPos pos, SqlNode period, SqlOperator operator) {
        super(pos, period, operator);
    }

    @Override
    public SqlOperator getOperator() {
        return STARTED_IN_OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

}
