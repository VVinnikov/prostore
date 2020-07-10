package ru.ibs.dtm.query.execution.core.calcite.ddl;

import java.util.List;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

public class DistributedOperator extends SqlCall {

    private static final SqlOperator DISTRIBUTED_OP =
            new SqlSpecialOperator("DISTRIBUTED BY", SqlKind.OTHER_DDL);
    private final SqlNodeList distributedBy;

    public DistributedOperator(SqlParserPos pos, SqlNodeList distributedBy) {
        super(pos);
        this.distributedBy = distributedBy;
    }

    @Override
    public SqlOperator getOperator() {
        return DISTRIBUTED_OP;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

    public SqlNodeList getDistributedBy() {
        return distributedBy;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (distributedBy != null) {
            writer.keyword(this.getOperator().getName());
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (SqlNode c : distributedBy) {
                writer.sep(",");
                c.unparse(writer, 0, 0);
            }
            writer.endList(frame);
        }
    }
}
