package io.arenadata.dtm.query.calcite.core.extension.ddl.truncate;

import com.google.common.collect.ImmutableList;
import io.arenadata.dtm.common.ddl.TruncateType;
import io.arenadata.dtm.query.calcite.core.util.CalciteUtil;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class SqlTruncateHistory extends SqlCall implements SqlBaseTruncate {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("TRUNCATE_HISTORY", SqlKind.OTHER_DDL);
    private static final String INFINITE = "infinite";
    private final SqlIdentifier name;
    private final String table;
    private final LocalDateTime datetime;
    private final boolean isInfinite;
    private final String conditions;

    public SqlTruncateHistory(SqlParserPos pos, SqlIdentifier name, SqlNode datetime, SqlNode conditions) {
        super(pos);
        this.name = name;
        String nameWithSchema = Objects.requireNonNull(name.toString());
        this.table = CalciteUtil.parseTableName(nameWithSchema);
        String datetimeStr = ((SqlCharStringLiteral) datetime).getNlsString().getValue();
        if (INFINITE.equalsIgnoreCase(datetimeStr)) {
            this.datetime = null;
            this.isInfinite = true;
        } else {
            this.datetime = CalciteUtil.parseLocalDateTime(datetimeStr);
            this.isInfinite = false;
        }
        this.conditions = Optional.ofNullable(conditions).map(SqlNode::toString).orElse(null);
    }

    @Override
    public TruncateType getTruncateType() {
        return TruncateType.HISTORY;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(name);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.literal(String.format("%s %s", this.getOperator(), this.name));
    }

    public String getTable() {
        return table;
    }

    public LocalDateTime getDateTime() {
        return datetime;
    }

    public boolean isInfinite() {
        return isInfinite;
    }

    public String getConditions() {
        return conditions;
    }
}
