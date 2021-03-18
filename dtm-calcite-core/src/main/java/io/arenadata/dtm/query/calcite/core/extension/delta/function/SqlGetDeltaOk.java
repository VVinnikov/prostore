package io.arenadata.dtm.query.calcite.core.extension.delta.function;

import io.arenadata.dtm.query.calcite.core.extension.delta.SqlDeltaCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlGetDeltaOk extends SqlDeltaCall {

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("GET_DELTA_OK", SqlKind.OTHER_DDL);

    public SqlGetDeltaOk(SqlParserPos pos) {
        super(pos);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }
}
