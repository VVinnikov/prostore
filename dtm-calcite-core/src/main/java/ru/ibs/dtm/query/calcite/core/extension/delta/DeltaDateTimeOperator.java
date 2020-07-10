package ru.ibs.dtm.query.calcite.core.extension.delta;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

public class DeltaDateTimeOperator extends SqlCall {

    private final String deltaDateTime;

    private static final SqlOperator OPERATOR_DELTA =
            new SqlSpecialOperator("SET", SqlKind.OTHER_DDL);

    public DeltaDateTimeOperator(SqlParserPos pos, SqlCharStringLiteral dateTime) {
        super(pos);
        this.deltaDateTime = dateTime != null ? dateTime.getNlsString().getValue() : null;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR_DELTA;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(this.getOperator().getName());
        writer.keyword(String.valueOf(this.deltaDateTime));
    }

    public String getDeltaDateTime() {
        return deltaDateTime;
    }
}
