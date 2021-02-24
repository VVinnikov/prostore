package io.arenadata.dtm.query.calcite.core.extension.check;

import com.google.common.collect.ImmutableList;
import lombok.val;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;
import java.util.List;

public abstract class SqlCheckCall extends SqlCall {
    protected final SqlNode name;

    public SqlCheckCall(SqlParserPos pos, SqlNode name) {
        super(pos);
        this.name = name;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(name);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        val nodeName = getNodeName();
        writer.literal(this.getOperator() + "(" + nodeName + ")");
    }

    private String getNodeName() {
        return this.name == null ? "" : "'" + this.name + "'";
    }

    @Nonnull
    @Override
    public abstract SqlOperator getOperator();

    public abstract CheckType getType();

    public abstract String getSchema();
}
