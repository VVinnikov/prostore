package io.arenadata.dtm.query.calcite.core.extension.snapshot;

import lombok.Getter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Optional;

@Getter
public class SnapshotDeltaNumOperator extends SqlCall {

    private final Long deltaNum;

    private static final SqlOperator DELTA_NUM_OPERATOR =
            new SqlSpecialOperator("DELTA_NUM", SqlKind.OTHER_DDL);
    private final SqlNumericLiteral deltaNumNode;

    public SnapshotDeltaNumOperator(SqlParserPos pos, SqlNumericLiteral deltaNumNode) {
        super(pos);
        this.deltaNumNode = deltaNumNode;
        this.deltaNum = Optional.ofNullable(this.deltaNumNode).map(c -> c.longValue(true)).orElse(null);
    }

    @Override
    public SqlOperator getOperator() {
        return DELTA_NUM_OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (deltaNum != null) {
            writer.keyword(this.getOperator().getName());
            writer.keyword(String.valueOf(this.deltaNum));
        }
    }

    public Long getDeltaNum() {
        return deltaNum;
    }
}
