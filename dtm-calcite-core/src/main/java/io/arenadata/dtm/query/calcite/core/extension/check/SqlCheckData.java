package io.arenadata.dtm.query.calcite.core.extension.check;

import io.arenadata.dtm.query.calcite.core.util.CalciteUtil;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SqlCheckData extends SqlCheckCall {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CHECK_DATA", SqlKind.CHECK);
    private final String table;
    private final String schema;
    private final Long deltaNum;
    private final Set<String> columns;

    public SqlCheckData(SqlParserPos pos, SqlNode name, SqlLiteral deltaNum, SqlNode columns) {
        super(pos, name);
        String nameWithSchema = Objects.requireNonNull(((SqlCharStringLiteral) name).getNlsString().getValue());
        this.schema = CalciteUtil.parseSchemaName(nameWithSchema);
        this.table = CalciteUtil.parseTableName(nameWithSchema);
        this.deltaNum = deltaNum.longValue(true);
        this.columns = Stream.of(Objects.requireNonNull(((SqlCharStringLiteral) columns).getNlsString().getValue())
                .split(","))
                .map(String::trim)
                .collect(Collectors.toSet());
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public CheckType getType() {
        return CheckType.DATA;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public Long getDeltaNum() {
        return deltaNum;
    }

    public Set<String> getColumns() {
        return columns;
    }
}
