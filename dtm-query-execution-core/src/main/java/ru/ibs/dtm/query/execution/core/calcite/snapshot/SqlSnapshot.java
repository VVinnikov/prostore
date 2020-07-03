package ru.ibs.dtm.query.execution.core.calcite.snapshot;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import ru.ibs.dtm.common.calcite.SnapshotType;

import java.util.List;
import java.util.Objects;

public class SqlSnapshot extends SqlCall {

    private final SnapshotOperator snapshotOperator;
    private SqlNode tableRef;
    private SqlNode period;
    private Boolean isLatestUncommitedDelta;

    public SqlSnapshot(SqlParserPos pos, SqlNode tableRef, SqlNode period) {
        super(pos);
        this.tableRef = (SqlNode) Objects.requireNonNull(tableRef);
        this.period = (SqlNode) Objects.requireNonNull(period);
        this.isLatestUncommitedDelta = isLatestUncommitedDelta(this.period);
        this.snapshotOperator = new SnapshotOperator();
    }

    @Override
    public SqlOperator getOperator() {
        return this.snapshotOperator;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(this.tableRef, this.period);
    }

    public SqlNode getTableRef() {
        return this.tableRef;
    }

    public SqlNode getPeriod() {
        return this.period;
    }

    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.tableRef = (SqlNode) Objects.requireNonNull(operand);
                break;
            case 1:
                this.period = (SqlNode) Objects.requireNonNull(operand);
                this.isLatestUncommitedDelta = isLatestUncommitedDelta(this.period);
                break;
            default:
                throw new AssertionError(i);
        }
    }

    private Boolean isLatestUncommitedDelta(SqlNode period){
        return period.toString().equals(SnapshotType.LATEST_UNCOMMITED_DELTA.toString().toLowerCase());
    }

    public Boolean getLatestUncommitedDelta() {
        return isLatestUncommitedDelta;
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        this.snapshotOperator.unparse(writer, this, 0, 0);
    }
}
