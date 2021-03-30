package io.arenadata.dtm.query.calcite.core.extension.config.function;

import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigCall;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Objects;

public class SqlConfigStorageAdd extends SqlConfigCall {
    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("CONFIG_STORAGE_ADD", SqlKind.OTHER_DDL);
    private final SourceType sourceType;

    public SqlConfigStorageAdd(SqlParserPos pos, SqlNode deltaDateTimeStr) {
        super(pos);
        this.sourceType = parseSourceType(Objects.requireNonNull((SqlCharStringLiteral) deltaDateTimeStr));
    }

    private SourceType parseSourceType(SqlCharStringLiteral stringLiteral) {
        String sourceTypeStr = stringLiteral.getNlsString().getValue().toUpperCase();
        return SourceType.valueOfAvailable(sourceTypeStr);
    }

    @Override
    public SqlConfigType getSqlConfigType() {
        return SqlConfigType.CONFIG_STORAGE_ADD;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.literal(
            OPERATOR + "(" + "'" +
                this.sourceType +
                "'" + ")");
    }

    public SourceType getSourceType() {
        return sourceType;
    }
}
