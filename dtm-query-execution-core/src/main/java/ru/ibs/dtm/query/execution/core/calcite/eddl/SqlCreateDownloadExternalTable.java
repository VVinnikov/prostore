package ru.ibs.dtm.query.execution.core.calcite.eddl;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Objects;

public class SqlCreateDownloadExternalTable extends SqlCreate {
  private final SqlIdentifier name;
  private final LocationOperator locationOperator;
  private final FormatOperator formatOperator;
  private final ChunkSizeOperator chunkSizeOperator;

  private static final SqlOperator OPERATOR_TABLE =
    new SqlSpecialOperator("CREATE DOWNLOAD EXTERNAL TABLE", SqlKind.OTHER_DDL);

  public SqlCreateDownloadExternalTable(SqlParserPos pos, boolean ifNotExists, SqlIdentifier name,
                                        SqlNode location, SqlNode format, SqlNode chunkSize) {
    super(OPERATOR_TABLE, pos, false, ifNotExists);
    this.name = Objects.requireNonNull(name);
    this.locationOperator = new LocationOperator(pos, (SqlCharStringLiteral) location);
    this.formatOperator = new FormatOperator(pos, (SqlCharStringLiteral) format);
    this.chunkSizeOperator = new ChunkSizeOperator(pos, (SqlNumericLiteral) chunkSize);
  }

  public List<SqlNode> getOperandList() {
    return ImmutableList.of(name, locationOperator, formatOperator, chunkSizeOperator);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(this.getOperator().getName());
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
    locationOperator.unparse(writer, leftPrec, rightPrec);
    formatOperator.unparse(writer, leftPrec, rightPrec);
    chunkSizeOperator.unparse(writer, leftPrec, rightPrec);
  }
}
