package io.arenadata.dtm.query.calcite.core.extension.ddl;

import com.google.common.collect.ImmutableList;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.util.SqlNodeUtil;
import lombok.Getter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

@Getter
public class SqlDropMaterializedView extends SqlDrop {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("DROP MATERIALIZED VIEW", SqlKind.DROP_MATERIALIZED_VIEW);
    private final SourceType destination;
    private final SqlIdentifier name;

    public SqlDropMaterializedView(SqlParserPos pos,
                                   boolean ifExists,
                                   SqlIdentifier name,
                                   SqlNode destination) {
        super(OPERATOR, pos, ifExists);
        this.name = name;
        this.destination = SqlNodeUtil.extractSourceType(destination);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(name);
    }

    @Override
    public void unparse(SqlWriter writer,
                        int leftPrec,
                        int rightPrec) {
        writer.keyword(this.getOperator().getName());
        if (this.ifExists) {
            writer.keyword("IF EXISTS");
        }

        this.name.unparse(writer, leftPrec, rightPrec);
    }
}
