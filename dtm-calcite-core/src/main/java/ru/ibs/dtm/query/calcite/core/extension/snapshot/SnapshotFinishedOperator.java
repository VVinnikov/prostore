package ru.ibs.dtm.query.calcite.core.extension.snapshot;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SnapshotFinishedOperator extends SnapshotDeltaIntervalOperator {

    private static final SqlOperator FINISHED_IN_OPERATOR =
            new SqlSpecialOperator("FINISHED IN", SqlKind.OTHER_DDL);

    public SnapshotFinishedOperator(SqlParserPos pos, SqlNode period, SqlOperator operator) {
        super(pos, period, operator);
    }

    @Override
    public SqlOperator getOperator() {
        return FINISHED_IN_OPERATOR;
    }

}