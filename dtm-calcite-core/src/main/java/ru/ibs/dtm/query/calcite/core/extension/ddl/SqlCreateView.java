package ru.ibs.dtm.query.calcite.core.extension.ddl;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import ru.ibs.dtm.query.calcite.core.extension.parser.ParseException;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class SqlCreateView extends SqlCreate {
    private final SqlIdentifier name;
    private final SqlNodeList columnList;
    private final SqlNode query;
    private static final SqlOperator OPERATOR;

    public SqlCreateView(SqlParserPos pos, boolean replace, SqlIdentifier name, SqlNodeList columnList, SqlNode query) throws ParseException {
        super(OPERATOR, pos, replace, false);
        this.name = (SqlIdentifier) Objects.requireNonNull(name);
        this.columnList = columnList;
        this.query = (SqlNode) checkQueryAndGet(Objects.requireNonNull(query));
    }

    private SqlNode checkQueryAndGet(SqlNode query) throws ParseException {
        if (query instanceof SqlSelect) {
            if (((SqlSelect) query).getFrom() == null) {
                throw new ParseException("View query must have from clause!");
            } else {
                return query;
            }
        } else {
            throw new ParseException(String.format("Type %s of query does not support!", query.getClass().getName()));
        }
    }

    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(this.name, this.columnList, this.query);
    }

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
            Iterator var5 = this.columnList.iterator();

            while (var5.hasNext()) {
                SqlNode c = (SqlNode) var5.next();
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
