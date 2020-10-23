package io.arenadata.dtm.query.calcite.core.dialect;

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

public class LimitSqlDialect extends PostgresqlSqlDialect {
    /**
     * Creates a PostgresqlSqlDialect.
     *
     * @param context
     */
    public LimitSqlDialect(Context context) {
        super(context);
    }

    @Override
    public void unparseOffsetFetch(SqlWriter writer, SqlNode offset, SqlNode fetch) {
        Preconditions.checkArgument(fetch != null || offset != null);
        if (offset != null) {
            writer.newlineAndIndent();
            final SqlWriter.Frame offsetFrame =
                writer.startList(SqlWriter.FrameTypeEnum.OFFSET);
            writer.keyword("OFFSET");
            offset.unparse(writer, -1, -1);
            writer.keyword("ROWS");
            writer.endList(offsetFrame);
        }
        if (fetch != null) {
            final SqlWriter.Frame fetchFrame =
                writer.startList(SqlWriter.FrameTypeEnum.FETCH);
            writer.newlineAndIndent();
            writer.keyword("LIMIT");
            fetch.unparse(writer, -1, -1);
            writer.endList(fetchFrame);
        }
    }
}
