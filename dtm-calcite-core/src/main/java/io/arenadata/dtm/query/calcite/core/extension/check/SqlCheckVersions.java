package io.arenadata.dtm.query.calcite.core.extension.check;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;

public class SqlCheckVersions extends SqlCheckCall {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CHECK_VERSIONS", SqlKind.CHECK);

    public SqlCheckVersions(SqlParserPos pos) {
        super(pos, null);
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public CheckType getType() {
        return CheckType.VERSIONS;
    }

    @Override
    public String getSchema() {
        return null;
    }

}
