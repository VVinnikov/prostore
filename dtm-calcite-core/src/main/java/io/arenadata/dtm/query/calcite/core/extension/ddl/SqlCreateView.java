package io.arenadata.dtm.query.calcite.core.extension.ddl;

import io.arenadata.dtm.query.calcite.core.extension.parser.ParseException;
import io.arenadata.dtm.query.calcite.core.util.SqlNodeUtil;
import lombok.Getter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

@Getter
public class SqlCreateView extends SqlCreate {
    private final SqlIdentifier name;
    private final SqlNodeList columnList;
    private final SqlNode query;
    private static final SqlOperator OPERATOR;

    public SqlCreateView(SqlParserPos pos, boolean replace, SqlIdentifier name, SqlNodeList columnList, SqlNode query) throws ParseException {
        super(OPERATOR, pos, replace, false);
        this.name = (SqlIdentifier) Objects.requireNonNull(name);
        this.columnList = columnList;
        this.query = SqlNodeUtil.getViewQueryAndCheck(Objects.requireNonNull(query));
    }

    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(this.name, this.columnList, this.query);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (this.getReplace()) {
            writer.keyword("CREATE OR REPLACE");
        } else {
            writer.keyword("CREATE");
        }

        writer.keyword("VIEW");
        this.name.unparse(writer, leftPrec, rightPrec);
        if (this.columnList != null) {
            SqlWriter.Frame frame = writer.startList("(", ")");
            Iterator<SqlNode> var5 = this.columnList.iterator();

            while (var5.hasNext()) {
                SqlNode c = var5.next();
                writer.sep(",");
                c.unparse(writer, 0, 0);
            }

            writer.endList(frame);
        }

        writer.keyword("AS");
        writer.newlineAndIndent();
        this.query.unparse(writer, 0, 0);
    }

    static {
        OPERATOR = new SqlSpecialOperator("CREATE VIEW", SqlKind.CREATE_VIEW);
    }
}
