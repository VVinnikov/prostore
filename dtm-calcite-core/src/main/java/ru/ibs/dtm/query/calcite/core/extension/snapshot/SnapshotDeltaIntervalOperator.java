package ru.ibs.dtm.query.calcite.core.extension.snapshot;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import ru.ibs.dtm.common.delta.DeltaInterval;

import java.util.List;

public class SnapshotDeltaIntervalOperator extends SqlCall {

    private SqlNode period;
    private DeltaInterval deltaInterval;
    private SqlOperator inOperator;

    private static final SqlOperator STARTED_IN_OPERATOR =
            new SqlSpecialOperator("", SqlKind.OTHER_DDL);

    public SnapshotDeltaIntervalOperator(SqlParserPos pos, SqlNode period, SqlOperator operator) {
        super(pos);
        this.period = period;
        this.inOperator = operator;
        this.deltaInterval = createDeltaInterval();
    }

    @Override
    public SqlOperator getOperator() {
        return STARTED_IN_OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (this.inOperator != null) {
            writer.keyword(this.getOperator().getName());
            writer.keyword(this.deltaInterval.getIntervalStr());
        }
    }

    public DeltaInterval getDeltaInterval() {
        return deltaInterval;
    }

    private DeltaInterval createDeltaInterval() {
        if (this.inOperator != null) {
            SqlBasicCall period = (SqlBasicCall) this.period;
            if (period.getOperands().length == 0 || period.getOperands().length > 2) {
                throw new RuntimeException("Delta interval must have two values!");
            }
            Long deltaFrom = Long.valueOf(String.valueOf(period.getOperands()[0]));
            Long deltaTo = Long.valueOf(String.valueOf(period.getOperands()[1]));
            if (deltaTo < deltaFrom) {
                throw new RuntimeException("Incorrect delta interval, deltaTo must be more than deltaFrom!");
            }
            return new DeltaInterval(deltaFrom, deltaTo);
        } else {
            return null;
        }
    }
}