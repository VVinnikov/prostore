package io.arenadata.dtm.query.calcite.core.extension.check;

import io.arenadata.dtm.query.calcite.core.util.CalciteUtil;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SqlCheckData extends SqlCheckCall {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CHECK_DATA", SqlKind.CHECK);
    private final String table;
    private final String schema;
    private final Long deltaNum;
    private final Set<String> columns;

    public SqlCheckData(SqlParserPos pos, SqlIdentifier name, SqlLiteral deltaNum, List<SqlNode> columns) {
        super(pos, name);
        final String nameWithSchema = name.toString();
        this.schema = CalciteUtil.parseSchemaName(nameWithSchema);
        this.table = CalciteUtil.parseTableName(nameWithSchema);
        this.deltaNum = deltaNum.longValue(true);
        this.columns = Optional.ofNullable(columns)
                .map(val -> (columns.stream()
                        .map(c -> ((SqlIdentifier) c))
                        .map(SqlIdentifier::getSimple)
                        .collect(Collectors.toCollection(LinkedHashSet::new))))
                .orElse(null);
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

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        String delimiter = ", ";
        String delta = this.deltaNum.toString();
        writer.literal(OPERATOR + "(" + getTableName() + delimiter + delta + getTableColumns(delimiter) + ")");
    }

    private String getTableColumns(String delimiter) {
        if (this.columns == null) {
            return "";
        } else {
            return delimiter + "[" + String.join(delimiter, this.columns) + "]";
        }

    }

    private String getTableName() {
        return this.name.toString();
    }
}
