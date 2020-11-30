package io.arenadata.dtm.query.calcite.core.extension.check;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;
import java.util.Optional;

public class SqlCheckDatabase extends SqlCheckCall {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CHECK_DATABASE", SqlKind.CHECK);
    private final String schema;

    public SqlCheckDatabase(SqlParserPos pos, SqlNode datamart) {
        super(pos, datamart);
        this.schema = Optional.ofNullable(name)
                .map(val -> ((SqlCharStringLiteral) val).getNlsString().getValue())
                .orElse(null);
    }

    public String getSchema() {
        return schema;
    }

    @Override
    public String getTable() {
        return null;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public CheckType getType() {
        return CheckType.DATABASE;
    }
}
