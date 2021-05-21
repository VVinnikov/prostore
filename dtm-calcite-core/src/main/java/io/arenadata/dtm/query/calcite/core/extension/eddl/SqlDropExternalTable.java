package io.arenadata.dtm.query.calcite.core.extension.eddl;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlDropExternalTable extends SqlDrop {

    private final SqlIdentifier name;

    public SqlDropExternalTable(SqlOperator operator, SqlParserPos pos, boolean ifExists, SqlIdentifier name) {
        super(operator, pos, ifExists);
        this.name = name;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(this.name);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(this.getOperator().getName());
        if (this.ifExists) {
            writer.keyword("IF EXISTS");
        }

        this.name.unparse(writer, leftPrec, rightPrec);
    }
}
