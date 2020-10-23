package io.arenadata.dtm.query.calcite.core.extension.ddl;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Objects;

public class SqlUseSchema extends SqlCall {

    private SqlIdentifier datamart;
    private static final SqlOperator USE_OPERATOR = new SqlSpecialOperator("USE", SqlKind.OTHER_DDL);

    public SqlUseSchema(SqlParserPos pos, SqlIdentifier datamart) {
        super(pos);
        this.datamart = Objects.requireNonNull(datamart);
    }

    @Override
    public SqlOperator getOperator() {
        return USE_OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(datamart);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(this.getOperator().getName());
        datamart.unparse(writer, leftPrec, rightPrec);
    }
}
