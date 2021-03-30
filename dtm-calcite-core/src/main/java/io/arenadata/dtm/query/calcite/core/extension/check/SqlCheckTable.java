package io.arenadata.dtm.query.calcite.core.extension.check;

import io.arenadata.dtm.query.calcite.core.util.CalciteUtil;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;
import java.util.Objects;

public class SqlCheckTable extends SqlCheckCall {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CHECK_TABLE", SqlKind.CHECK);
    private final String table;
    private final String schema;

    public SqlCheckTable(SqlParserPos pos, SqlIdentifier id) {
        super(pos, id);
        String nameWithSchema = Objects.requireNonNull(id.toString());
        this.schema = CalciteUtil.parseSchemaName(nameWithSchema);
        this.table = CalciteUtil.parseTableName(nameWithSchema);
    }

    public String getTable() {
        return table;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public CheckType getType() {
        return CheckType.TABLE;
    }
}
