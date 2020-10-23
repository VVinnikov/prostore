package io.arenadata.dtm.query.calcite.core.extension.eddl;

import io.arenadata.dtm.common.plugin.exload.Format;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

public class FormatOperator extends SqlCall {

    private static final SqlOperator OPERATOR_FORMAT =
            new SqlSpecialOperator("FORMAT", SqlKind.OTHER_DDL);
    private final Format format;

    FormatOperator(SqlParserPos pos, SqlCharStringLiteral format) {
        super(pos);
        this.format = Format.findByName(format.getNlsString().getValue());

    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR_FORMAT;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(this.getOperator().getName());
        writer.keyword("'" + this.format.getName() + "'");
    }

    public Format getFormat() {
        return format;
    }
}
