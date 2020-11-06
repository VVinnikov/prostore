package io.arenadata.dtm.query.calcite.core.extension.ddl;

import com.google.common.collect.ImmutableList;
import io.arenadata.dtm.common.reader.SourceType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SqlDropTable extends SqlDrop {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("DROP TABLE", SqlKind.DROP_TABLE);
    private final Set<SourceType> destination;
    private final SqlIdentifier name;

    public SqlDropTable(SqlParserPos pos,
                        boolean ifExists,
                        SqlIdentifier name,
                        SqlNodeList destination) {
        super(OPERATOR, pos, ifExists);
        this.name = name;
        this.destination = Optional.ofNullable(destination)
                .map(nodeList -> nodeList.getList().stream()
                        .map(node -> SourceType.valueOfAvailable(node.toString()))
                        .collect(Collectors.toSet()))
                .orElse(null);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(name);
    }

    public Set<SourceType> getDestination() {
        return destination;
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
