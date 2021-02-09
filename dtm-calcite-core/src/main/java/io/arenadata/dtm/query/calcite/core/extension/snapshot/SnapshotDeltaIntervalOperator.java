package io.arenadata.dtm.query.calcite.core.extension.snapshot;

import io.arenadata.dtm.common.delta.SelectOnInterval;
import lombok.Getter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

@Getter
public class SnapshotDeltaIntervalOperator extends SqlCall {

    private SqlNode period;
    private SelectOnInterval selectOnInterval;
    private SqlOperator inOperator;

    private static final SqlOperator STARTED_IN_OPERATOR =
            new SqlSpecialOperator("", SqlKind.OTHER_DDL);

    public SnapshotDeltaIntervalOperator(SqlParserPos pos, SqlNode period, SqlOperator operator) {
        super(pos);
        this.period = period;
        this.inOperator = operator;
        this.selectOnInterval = createDeltaInterval();
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
            writer.keyword(this.selectOnInterval.getIntervalStr());
        }
    }

    public SelectOnInterval getDeltaInterval() {
        return selectOnInterval;
    }

    private SelectOnInterval createDeltaInterval() {
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
            return new SelectOnInterval(deltaFrom, deltaTo);
        } else {
            return null;
        }
    }
}
