package io.arenadata.dtm.query.calcite.core.extension.delta.function;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SqlGetDeltaByDateTime extends SqlCall {

    private String deltaDateTime;
    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("GET_DELTA_BY_DATETIME", SqlKind.OTHER_DDL);

    public SqlGetDeltaByDateTime(SqlParserPos pos, SqlNode deltaDateTimeStr) {
        super(pos);
        this.deltaDateTime = getDeltaDateTime(Objects.requireNonNull((SqlCharStringLiteral)deltaDateTimeStr));
    }

    private String getDeltaDateTime(SqlCharStringLiteral deltaDateTimeStr) {
        return deltaDateTimeStr.getNlsString().getValue();
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.literal(
                OPERATOR + "(" + "'" +
                        this.deltaDateTime +
                        "'" + ")");
    }

    public String getDeltaDateTime() {
        return deltaDateTime;
    }
}
