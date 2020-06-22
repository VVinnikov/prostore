package ru.ibs.dtm.query.execution.core.calcite.delta;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;
import java.util.List;

public class SqlBeginDelta extends SqlCall {

    private DeltaNumOperator deltaNumOperator;
    private static final SqlOperator BEGIN_DELTA_OPERATOR =
            new SqlSpecialOperator("BEGIN DELTA", SqlKind.OTHER_DDL);

    public SqlBeginDelta(SqlParserPos pos, SqlNode num) {
        super(pos);
        this.deltaNumOperator = new DeltaNumOperator(pos, (SqlNumericLiteral) num);
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return BEGIN_DELTA_OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(deltaNumOperator);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(this.getOperator().getName());
        deltaNumOperator.unparse(writer, leftPrec, rightPrec);
    }

    public DeltaNumOperator getDeltaNumOperator() {
        return deltaNumOperator;
    }
}
