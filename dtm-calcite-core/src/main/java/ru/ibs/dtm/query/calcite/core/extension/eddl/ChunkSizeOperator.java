package ru.ibs.dtm.query.calcite.core.extension.eddl;

import java.util.List;
import java.util.Optional;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

public class ChunkSizeOperator extends SqlCall {

    private static final SqlOperator OPERATOR_CHUNK_SIZE =
            new SqlSpecialOperator("CHUNK_SIZE", SqlKind.OTHER_DDL);
    private final Integer chunkSize;

    public ChunkSizeOperator(SqlParserPos pos, SqlNumericLiteral chunkSize) {
        super(pos);
        this.chunkSize = Optional.ofNullable(chunkSize).map(c -> c.intValue(true)).orElse(null);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR_CHUNK_SIZE;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

    public Integer getChunkSize() {
        return chunkSize;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (chunkSize != null) {
            writer.keyword(this.getOperator().getName());
            writer.keyword(String.valueOf(this.chunkSize));
        }
    }
}
