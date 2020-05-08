package ru.ibs.dtm.query.execution.core.calcite.eddl;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Optional;

public class ChunkSizeOperator extends SqlCall {

  private final Integer chunkSize;

  private static final SqlOperator OPERATOR_CHUNK_SIZE =
    new SqlSpecialOperator("CHUNK_SIZE", SqlKind.OTHER_DDL);

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
    writer.keyword(this.getOperator().getName());
    writer.keyword(String.valueOf(this.chunkSize));
  }
}
