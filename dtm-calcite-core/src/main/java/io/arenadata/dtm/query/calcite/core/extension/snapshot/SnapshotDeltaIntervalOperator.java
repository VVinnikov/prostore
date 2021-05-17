package io.arenadata.dtm.query.calcite.core.extension.snapshot;

import io.arenadata.dtm.common.delta.SelectOnInterval;
import io.arenadata.dtm.common.exception.DtmException;
import lombok.Getter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

@Getter
public class SnapshotDeltaIntervalOperator extends SqlCall {

    private final SqlNode period;
    private final SelectOnInterval selectOnInterval;
    private final SqlOperator inOperator;

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
            SqlBasicCall periodCall = (SqlBasicCall) this.period;
            if (periodCall.getOperands().length == 0 || periodCall.getOperands().length > 2) {
                throw new DtmException("Delta interval must have two values!");
            }
            Long deltaFrom = Long.valueOf(String.valueOf(periodCall.getOperands()[0]));
            Long deltaTo = Long.valueOf(String.valueOf(periodCall.getOperands()[1]));
            if (deltaTo < deltaFrom) {
                throw new DtmException("Incorrect delta interval, deltaTo must be more than deltaFrom!");
            }
            return new SelectOnInterval(deltaFrom, deltaTo);
        } else {
            return null;
        }
    }
}
