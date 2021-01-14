package io.arenadata.dtm.query.calcite.core.extension.snapshot;

import io.arenadata.dtm.common.delta.SelectOnInterval;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Objects;

public class SqlDeltaSnapshot extends SqlSnapshot {

    public static final String AS_OF = "AS OF";
    private final SnapshotOperator snapshotOperator;
    private SqlNode tableRef;
    private SqlNode period;
    private String deltaDateTime;
    private Boolean isLatestUncommittedDelta;
    private SelectOnInterval startedInterval;
    private SelectOnInterval finishedInterval;
    private Long deltaNum;
    private final SnapshotDeltaIntervalOperator startedOperator;
    private final SnapshotDeltaIntervalOperator finishedOperator;
    private final SnapshotDeltaNumOperator deltaNumOperator;
    private final SnapshotLatestUncommittedDeltaOperator latestUncommittedDeltaOperator;

    public SqlDeltaSnapshot(SqlParserPos pos, SqlNode tableRef, SqlNode period, SqlOperator started,
                            SqlOperator finished, SqlNode num, SqlLiteral isLatestUncommittedDelta) {
        super(pos, tableRef, period);
        this.tableRef = (SqlNode) Objects.requireNonNull(tableRef);
        this.period = (SqlNode) period;
        this.snapshotOperator = new SnapshotOperator();
        this.startedOperator = new SnapshotStartedOperator(pos, this.period, started);
        this.finishedOperator = new SnapshotFinishedOperator(pos, this.period, finished);
        this.deltaNumOperator = new SnapshotDeltaNumOperator(pos, (SqlNumericLiteral) num);
        this.latestUncommittedDeltaOperator = new SnapshotLatestUncommittedDeltaOperator(pos, isLatestUncommittedDelta);
        initSnapshotAttributes();
    }

    @Override
    public SqlOperator getOperator() {
        return this.snapshotOperator;
    }

    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.tableRef = (SqlNode) Objects.requireNonNull(operand);
                break;
            case 1:
                this.period = (SqlNode) Objects.requireNonNull(operand);
                initSnapshotAttributes();
                break;
            default:
                throw new AssertionError(i);
        }
    }

    private void initSnapshotAttributes() {
        this.startedInterval = this.startedOperator.getDeltaInterval();
        this.finishedInterval = this.finishedOperator.getDeltaInterval();
        this.deltaNum = this.deltaNumOperator.getDeltaNum();
        this.isLatestUncommittedDelta = this.latestUncommittedDeltaOperator.getIsLatestUncommittedDelta();
        this.deltaDateTime = createDeltaDateTime();
    }

    private String createDeltaDateTime() {
        if (this.deltaNum != null ||
                this.startedInterval != null || this.finishedInterval != null || this.isLatestUncommittedDelta) {
            return null;
        } else {
            return this.period.toString();
        }
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        this.snapshotOperator.unparse(writer, this, 0, rightPrec);
        if (this.getStartedInterval() == null && this.getFinishedInterval() == null) {
            writer.keyword(AS_OF);
        }
        this.deltaNumOperator.unparse(writer, 0, 0);
        this.startedOperator.unparse(writer, 0, 0);
        this.finishedOperator.unparse(writer, 0, 0);
        this.latestUncommittedDeltaOperator.unparse(writer, 0, 0);
        if (this.getDeltaDateTime() != null) {
            this.period.unparse(writer, 0, 0);
        }
    }

    public SqlNode getTableRef() {
        return this.tableRef;
    }

    public SelectOnInterval getStartedInterval() {
        return startedInterval;
    }

    public SelectOnInterval getFinishedInterval() {
        return finishedInterval;
    }

    public String getDeltaDateTime() {
        return deltaDateTime;
    }

    public Boolean getLatestUncommittedDelta() {
        return isLatestUncommittedDelta;
    }

    public Long getDeltaNum() {
        return deltaNum;
    }
}