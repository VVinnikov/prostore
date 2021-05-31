package io.arenadata.dtm.query.calcite.core.extension.ddl;

import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.parser.ParseException;
import io.arenadata.dtm.query.calcite.core.util.SqlNodeUtil;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class SqlCreateMaterializedView extends SqlCreate {
    private final SqlIdentifier name;
    private final SqlNodeList columnList;
    private final SqlNode query;
    private final DistributedOperator distributedBy;
    private final Set<SourceType> destination;

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("CREATE MATERIALIZED VIEW",
                    SqlKind.CREATE_MATERIALIZED_VIEW);

    /**
     * Creates a SqlCreateMaterializedView.
     */
    public SqlCreateMaterializedView(SqlParserPos pos,
                                     SqlIdentifier name,
                                     SqlNodeList columnList,
                                     SqlNodeList distributedBy,
                                     SqlNodeList destination,
                                     SqlNode query) throws ParseException {
        super(OPERATOR, pos, false, false);
        this.name = Objects.requireNonNull(name);
        this.columnList = columnList;
        this.query = SqlNodeUtil.checkViewQueryAndGet(Objects.requireNonNull(query));
        this.distributedBy = new DistributedOperator(pos, distributedBy);
        this.destination = SqlNodeUtil.extractSourceTypes(destination);
    }

    public SqlCreateMaterializedView(SqlParserPos pos,
                                     SqlIdentifier name,
                                     SqlNodeList columnList,
                                     DistributedOperator distributedBy,
                                     Set<SourceType> destination,
                                     SqlNode query) throws ParseException {
        super(OPERATOR, pos, false, false);
        this.name = Objects.requireNonNull(name);
        this.columnList = columnList;
        this.query = SqlNodeUtil.checkViewQueryAndGet(Objects.requireNonNull(query));
        this.distributedBy = distributedBy;
        this.destination = destination;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columnList, query, distributedBy);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(this.getOperator().getName());
        name.unparse(writer, leftPrec, rightPrec);
        if (columnList != null) {
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (SqlNode c : columnList) {
                writer.sep(",");
                c.unparse(writer, 0, 0);
            }
            writer.endList(frame);
        }
        if (distributedBy != null) {
            distributedBy.unparse(writer, 0, 0);
        }
        writer.keyword("AS");
        writer.newlineAndIndent();
        query.unparse(writer, 0, 0);
    }

    public SqlIdentifier getName() {
        return name;
    }

    public SqlNodeList getColumnList() {
        return columnList;
    }

    public SqlNode getQuery() {
        return query;
    }

    public DistributedOperator getDistributedBy() {
        return distributedBy;
    }

    public Set<SourceType> getDestination() {
        return destination;
    }
}
